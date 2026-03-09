"""AI.CHAT Device Manager — authenticates device, receives commands via SSE,
launches and monitors Agent SDK worker processes per channel.

Usage:
    uv run python3 aichat_manager.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import sys
import time
from dataclasses import dataclass

import httpx

from aichat_device_auth import (
    DeviceConfig,
    generate_device_keypair,
    load_device_config,
    poll_for_approval,
    register_device,
    save_device_config,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass
class WorkerProcess:
    """Tracks a running worker subprocess."""

    proc: asyncio.subprocess.Process
    channel_id: str
    channel_token: str
    started_at: float = 0.0

    def __post_init__(self):
        if not self.started_at:
            self.started_at = time.time()


def parse_device_command(event: dict) -> dict | None:
    """Parse a notification event, returning command data if it's a device command."""
    if event.get("type") != "aichat:device-command":
        return None
    command = event.get("command")
    if not command:
        return None
    return {
        "command": command,
        "payload": event.get("payload", {}),
    }


class DeviceManager:
    """Manages worker processes on this device."""

    def __init__(self, device_id: str, private_key_b64: str, base_url: str):
        self.device_id = device_id
        self.private_key_b64 = private_key_b64
        self.base_url = base_url
        self.workers: dict[str, WorkerProcess] = {}
        self.last_event_ts: str | None = None

    async def start_worker(self, channel_id: str, channel_token: str) -> None:
        """Launch a worker subprocess for the given channel."""
        # Stop existing worker for this channel if any
        if channel_id in self.workers:
            await self.stop_worker(channel_id)

        env = {**os.environ, "AICHAT_PRIVATE_KEY": channel_token}
        env.pop("CLAUDECODE", None)  # Prevent nested Claude Code detection

        proc = await asyncio.create_subprocess_exec(
            sys.executable, "aichat_agent.py",
            env=env,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        self.workers[channel_id] = WorkerProcess(
            proc=proc, channel_id=channel_id, channel_token=channel_token
        )
        log.info("Started worker for channel %s (pid=%s)", channel_id, proc.pid)

    async def stop_worker(self, channel_id: str) -> None:
        """Gracefully stop a worker."""
        worker = self.workers.pop(channel_id, None)
        if not worker:
            return
        log.info("Stopping worker for channel %s", channel_id)
        worker.proc.terminate()
        try:
            await asyncio.wait_for(worker.proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            log.warning("Worker %s did not exit in time, killing", channel_id)
            worker.proc.kill()
            await worker.proc.wait()

    async def handle_command(self, cmd: dict) -> None:
        """Handle a parsed device command."""
        command = cmd["command"]
        payload = cmd.get("payload", {})

        if command == "worker:start":
            await self.start_worker(payload["channel_id"], payload["channel_token"])
        elif command == "worker:stop":
            await self.stop_worker(payload["channel_id"])
        elif command == "worker:restart":
            channel_id = payload["channel_id"]
            token = self.workers.get(channel_id, None)
            channel_token = token.channel_token if token else payload.get("channel_token", "")
            await self.start_worker(channel_id, channel_token)
        elif command == "device:ping":
            log.info("Ping received, reporting status")
            await self.report_status()
        elif command == "device:update":
            log.info("Device update: %s", payload)
        else:
            log.warning("Unknown command: %s", command)

    def get_status(self) -> dict:
        """Get current device status."""
        workers = []
        for channel_id, worker in self.workers.items():
            status = "running" if worker.proc.returncode is None else "crashed"
            workers.append({
                "channel_id": channel_id,
                "status": status,
                "uptime": int(time.time() - worker.started_at),
            })
        return {
            "status": "online",
            "device_id": self.device_id,
            "workers": workers,
        }

    async def report_status(self) -> None:
        """Report device status to the server."""
        status = self.get_status()
        try:
            headers = self._sign_request("POST", "/api/device/status")
            headers["Content-Type"] = "application/json"
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{self.base_url}/api/device/status",
                    json=status,
                    headers=headers,
                    timeout=10,
                )
        except Exception as e:
            log.warning("Failed to report status: %s", e)

    def _sign_request(self, method: str, path: str) -> dict[str, str]:
        """Sign a request with device Ed25519 key."""
        import base64 as _b64
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as _K

        key_bytes = _b64.b64decode(self.private_key_b64)
        private_key = _K.from_private_bytes(key_bytes)

        timestamp = str(int(time.time()))
        message = f"{timestamp}.{method.upper()}.{path}".encode()
        signature = private_key.sign(message)

        return {
            "X-Timestamp": timestamp,
            "X-Signature": _b64.b64encode(signature).decode(),
            "X-Device-Id": self.device_id,
        }

    async def _check_workers(self) -> None:
        """Check for crashed workers and restart them."""
        for channel_id, worker in list(self.workers.items()):
            if worker.proc.returncode is not None:
                log.warning("Worker %s crashed (rc=%s), restarting", channel_id, worker.proc.returncode)
                await self.start_worker(channel_id, worker.channel_token)

    async def monitor_workers(self) -> None:
        """Continuously monitor workers for crashes."""
        while True:
            await self._check_workers()
            await asyncio.sleep(5)

    async def listen_sse(self, cookies: httpx.Cookies) -> None:
        """Listen to Skrift notification stream for device commands."""
        while True:
            try:
                url = f"{self.base_url}/notifications/stream"
                if self.last_event_ts:
                    url += f"?since={self.last_event_ts}"

                log.info("Connecting to SSE stream...")
                async with httpx.AsyncClient(
                    cookies=cookies,
                    timeout=httpx.Timeout(None, connect=10),
                ) as client:
                    async with client.stream("GET", url) as response:
                        async for line in response.aiter_lines():
                            if not line.startswith("data:"):
                                continue
                            try:
                                event = json.loads(line[5:].strip())
                            except (json.JSONDecodeError, ValueError):
                                continue

                            # Track timestamp for reconnect replay
                            if "timestamp" in event:
                                self.last_event_ts = event["timestamp"]

                            cmd = parse_device_command(event)
                            if cmd:
                                log.info("Received command: %s", cmd["command"])
                                await self.handle_command(cmd)

            except httpx.HTTPError as e:
                log.warning("SSE connection error: %s, reconnecting in 5s", e)
                await asyncio.sleep(5)
            except Exception as e:
                log.error("SSE unexpected error: %s, reconnecting in 10s", e)
                await asyncio.sleep(10)

    async def shutdown(self) -> None:
        """Gracefully stop all workers."""
        log.info("Shutting down, stopping %d workers", len(self.workers))
        for channel_id in list(self.workers.keys()):
            await self.stop_worker(channel_id)


async def get_session_for_device(config: DeviceConfig) -> httpx.Cookies:
    """Exchange device auth for a session cookie.

    Uses the device's Ed25519 key to sign a request to POST /api/device/session,
    getting back a session cookie for SSE stream access.
    """
    import base64
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key_bytes = base64.b64decode(config.private_key_b64)
    private_key = Ed25519PrivateKey.from_private_bytes(key_bytes)

    path = "/api/device/session"
    timestamp = str(int(time.time()))
    message = f"{timestamp}.POST.{path}".encode()
    signature = private_key.sign(message)

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": base64.b64encode(signature).decode(),
        "X-Device-Id": config.device_id,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{config.base_url}{path}", headers=headers)
        resp.raise_for_status()
        return resp.cookies


async def device_auth_flow(base_url: str) -> DeviceConfig:
    """Run the interactive device auth flow."""
    device_name = platform.node() or "Unknown Device"
    keypair = generate_device_keypair()

    log.info("Registering device '%s'...", device_name)
    result = await register_device(
        base_url=base_url,
        public_key_b64=keypair["public_key_b64"],
        device_name=device_name,
    )

    auth_url = result["auth_url"]
    device_code = result["device_code"]
    print(f"\nAuthorize this device: {auth_url}\n")

    # Try to open browser
    try:
        import webbrowser
        webbrowser.open(auth_url)
    except Exception:
        pass

    # Poll for approval
    log.info("Waiting for approval...")
    while True:
        status = await poll_for_approval(base_url, device_code)
        if status["status"] == "approved":
            log.info("Device approved!")
            config = DeviceConfig(
                device_id=status["device_id"],
                device_name=device_name,
                private_key_b64=keypair["private_key_b64"],
                public_key_b64=keypair["public_key_b64"],
                base_url=base_url,
            )
            save_device_config(config)
            return config
        elif status["status"] == "denied":
            raise SystemExit("Device authorization denied.")
        # Still pending
        await asyncio.sleep(2)


async def run_manager() -> None:
    """Main manager entry point."""
    base_url = os.environ.get("AICHAT_URL", "https://aichat.zech.sh")

    # Load or create device config
    config = load_device_config()
    if not config:
        config = await device_auth_flow(base_url)

    log.info("Device: %s (%s)", config.device_name, config.device_id)

    # Get session cookie for SSE
    cookies = await get_session_for_device(config)

    manager = DeviceManager(
        device_id=config.device_id,
        private_key_b64=config.private_key_b64,
        base_url=config.base_url,
    )

    # Report online
    await manager.report_status()

    try:
        # Run SSE listener and worker monitor concurrently
        async with asyncio.TaskGroup() as tg:
            tg.create_task(manager.listen_sse(cookies))
            tg.create_task(manager.monitor_workers())
    except KeyboardInterrupt:
        pass
    finally:
        await manager.shutdown()


def main() -> None:
    asyncio.run(run_manager())


if __name__ == "__main__":
    main()
