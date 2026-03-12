"""AI.CHAT Device Manager — authenticates device, connects to server via
WebSocket, launches and monitors Agent SDK worker processes per channel.

Workers communicate exclusively through the manager via IPC (Unix domain sockets).
The manager proxies their API requests to the server over a single WebSocket and
forwards incoming notification events to the appropriate worker.

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
import uuid
from dataclasses import dataclass

import httpx
import websockets

from aichat_api import AiChatAPI
from aichat_crypto import encrypt, encrypt_channel_key, decrypt, generate_channel_key
from aichat_db import LocalDB
from aichat_device_auth import (
    DeviceConfig,
    generate_device_keypair,
    load_device_config,
    poll_for_approval,
    register_device,
    save_device_config,
)
from aichat_ipc import (
    IPCServer,
    MSG_SEND_MESSAGE,
    MSG_TOOL_STATUS,
    MSG_MARK_READ,
    MSG_CREATE_INTERACTION,
    MSG_SEND_EVENT,
    MSG_REPORT_DIRECTORIES,
    MSG_DOWNLOAD_ATTACHMENT,
    MSG_EVENT_MESSAGE,
    MSG_EVENT_PLAN,
    MSG_EVENT_INTERACTION_RESPONSE,
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
    working_directory: str = ""

    def __post_init__(self):
        if not self.started_at:
            self.started_at = time.time()


def parse_device_command(event: dict) -> dict | None:
    """Parse a notification event, returning command data if it's a device command."""
    if event.get("event_type") != "aichat:device-command":
        return None
    command = event.get("command")
    if not command:
        return None
    return {
        "command": command,
        "payload": event.get("payload", {}),
    }


def _parse_worker_event(event: dict, channel_id: str) -> dict | None:
    """Parse a notification event into an IPC message for a worker, or None if irrelevant."""
    # Only forward events for this channel
    if event.get("channel_id") and event.get("channel_id") != channel_id:
        return None

    event_type = event.get("event_type")

    # User messages
    if event_type == "aichat:message" and event.get("sender") == "user":
        return {
            "type": MSG_EVENT_MESSAGE,
            "channel_id": channel_id,
            "message_id": event.get("message_id"),
            "content": event.get("content", ""),
            "sender": "user",
            "attachments": event.get("attachments", []),
        }

    # Plan mode events
    if (
        event_type == "aichat:message"
        and event.get("sender") == "event"
        and event.get("content", "").startswith("plan:")
    ):
        return {
            "type": MSG_EVENT_PLAN,
            "channel_id": channel_id,
            "content": event.get("content", ""),
        }

    # Interaction responses
    if event_type == "aichat:interaction-response":
        return {
            "type": MSG_EVENT_INTERACTION_RESPONSE,
            "channel_id": channel_id,
            "interaction_id": event.get("interaction_id"),
            "action": event.get("action"),
            "answer": event.get("answer", ""),
            "reason": event.get("reason", ""),
        }

    return None


class DeviceManager:
    """Manages worker processes on this device."""

    def __init__(self, device_id: str, private_key_b64: str, base_url: str,
                 device_master_key_b64: str = "",
                 x25519_private_b64: str = ""):
        self.device_id = device_id
        self.private_key_b64 = private_key_b64
        self.base_url = base_url
        self.device_master_key_b64 = device_master_key_b64
        self.x25519_private_b64 = x25519_private_b64
        self.workers: dict[str, WorkerProcess] = {}
        self.last_event_ts: str | None = None

        # Local database for messages and channels
        self.local_db = LocalDB()

        # IPC server (initialized in start_ipc())
        self.ipc: IPCServer | None = None
        self.ipc_socket_path: str = f"/tmp/aichat-{device_id}.sock"

        # WebSocket connection state
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._ws_pending: dict[str, asyncio.Future] = {}  # rid → Future
        self._ws_lock = asyncio.Lock()  # serialize sends

    # ------------------------------------------------------------------
    # WebSocket communication
    # ------------------------------------------------------------------

    async def connect_websocket(self) -> None:
        """Connect to server via WebSocket with automatic reconnection."""
        backoff = 1
        while True:
            try:
                headers = self._sign_request("GET", "/api/device/ws")
                ws_url = self.base_url.replace("https://", "wss://").replace("http://", "ws://")
                ws_url += "/api/device/ws"

                log.info("Connecting to WebSocket at %s ...", ws_url)
                async with websockets.connect(
                    ws_url, additional_headers=headers,
                ) as ws:
                    self._ws = ws
                    backoff = 1  # reset on successful connect
                    log.info("WebSocket connected")

                    # Start the read loop first so responses can be dispatched,
                    # then run initial sync. Both run concurrently.
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._ws_read_loop(ws))
                        tg.create_task(self._initial_sync())

            except Exception as e:
                log.warning("WebSocket error: %s, reconnecting in %ds", e, backoff)
                self._ws = None
                # Fail all pending requests
                for future in self._ws_pending.values():
                    if not future.done():
                        future.set_exception(ConnectionError("WebSocket disconnected"))
                self._ws_pending.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _ws_read_loop(self, ws) -> None:
        """Read messages from WebSocket and dispatch responses/events."""
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue

            msg_type = msg.get("type")

            if msg_type == "response":
                rid = msg.get("rid")
                future = self._ws_pending.pop(rid, None) if rid else None
                if future and not future.done():
                    future.set_result(msg)
                continue

            if msg_type == "event":
                log.info("WS event received: event_type=%s", msg.get("event_type"))
                await self._handle_ws_event(msg)
                continue

            log.warning("Unknown WS message type: %s", msg_type)

    async def _initial_sync(self) -> None:
        """Run initial sync after WebSocket connect."""
        await self.report_status()
        await self._push_x25519_public()
        await self.sync_channels()

    async def _push_x25519_public(self) -> None:
        """Push device X25519 public key to server if available."""
        if not self.x25519_private_b64:
            return
        config = load_device_config()
        if not config or not config.x25519_public_b64:
            return
        try:
            await self._ws_request({
                "type": "update_device_x25519",
                "x25519_public": config.x25519_public_b64,
            })
            log.info("Pushed X25519 public key to server")
        except Exception as e:
            log.warning("Failed to push X25519 public key: %s", e)

    async def _ws_request(self, msg: dict, timeout: float = 30.0) -> dict:
        """Send a request over WebSocket and await the correlated response."""
        if not self._ws:
            raise ConnectionError("WebSocket not connected")

        rid = uuid.uuid4().hex[:12]
        msg["rid"] = rid

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._ws_pending[rid] = future

        try:
            async with self._ws_lock:
                await self._ws.send(json.dumps(msg))
            result = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._ws_pending.pop(rid, None)
            raise TimeoutError(f"WS request timed out: {msg.get('type')}")

        if not result.get("ok"):
            raise RuntimeError(f"WS request failed: {result.get('error')}")
        return result.get("data", {})

    async def _ws_fire_and_forget(self, msg: dict) -> None:
        """Send a message over WebSocket without awaiting a response."""
        if not self._ws:
            return  # Silently drop if not connected (e.g. tool_status during reconnect)
        try:
            async with self._ws_lock:
                await self._ws.send(json.dumps(msg))
        except Exception:
            pass  # Best-effort for fire-and-forget

    async def _handle_ws_event(self, event: dict) -> None:
        """Handle a notification event received over WebSocket."""
        # Device commands → handle locally
        cmd = parse_device_command(event)
        if cmd:
            log.info("Received command: %s", cmd["command"])
            await self.handle_command(cmd)
            return

        # Re-key requests from new browser sessions
        event_type = event.get("event_type")
        if event_type == "aichat:rekey-request":
            await self._handle_rekey_request(event)
            return

        # History requests from browser
        if event_type == "aichat:history-request":
            await self._handle_history_request(event)
            return

        # Track timestamp for potential replay
        if "timestamp" in event:
            self.last_event_ts = event["timestamp"]

        # Dual-write: store incoming messages locally
        await self._store_event_locally(event)

        # Forward worker-relevant events via IPC
        if self.ipc:
            await self._forward_event_to_worker(event)

    async def _handle_rekey_request(self, event: dict) -> None:
        """Handle a re-key request from a new browser session.

        Browser sends its X25519 public key. We compute a new shared secret,
        re-encrypt all channel keys, and send them back.
        """
        from aichat_crypto import derive_device_master_key_b64 as derive_key

        browser_x25519_public = event.get("browser_x25519_public", "")
        request_id = event.get("request_id", "")

        if not browser_x25519_public or not self.x25519_private_b64:
            log.warning("Re-key request missing browser key or device X25519 key")
            return

        try:
            # Compute new shared secret with the browser's ephemeral key
            new_master = derive_key(self.x25519_private_b64, browser_x25519_public)

            # Update in-memory master key and persist to device.json
            self.device_master_key_b64 = new_master
            config = load_device_config()
            if config:
                config.device_master_key_b64 = new_master
                save_device_config(config)

            # Re-encrypt all channel keys with the new master key
            encrypted_keys = {}
            channels = await self.local_db.list_channels()
            for ch in channels:
                ch_key = ch.get("channel_key_b64")
                if ch_key:
                    ct, nonce = encrypt_channel_key(new_master, ch_key)
                    encrypted_keys[ch["id"]] = {
                        "encrypted_key": ct,
                        "nonce": nonce,
                    }
                    # Also upload to server so chat page can serve them
                    await self._upload_encrypted_channel_key(ch["id"], ch_key)

            # Send back via WebSocket
            await self._ws_fire_and_forget({
                "type": "rekey_response",
                "request_id": request_id,
                "encrypted_keys": encrypted_keys,
            })
            log.info("Sent re-key response with %d channel keys", len(encrypted_keys))

        except Exception as e:
            log.warning("Re-key failed: %s", e)

    async def _handle_history_request(self, event: dict) -> None:
        """Handle a history request from the browser, proxied via server.

        Query local SQLite and send encrypted messages back.
        """
        channel_id = event.get("channel_id", "")
        request_id = event.get("request_id", "")
        before_id = event.get("before")
        limit = min(event.get("limit", 100), 200)

        if not channel_id or not request_id:
            return

        try:
            messages = await self.local_db.get_messages(
                channel_id, limit=limit + 1, before_id=before_id
            )
            has_more = len(messages) > limit
            messages = messages[:limit]

            # Encrypt each message for the browser
            channel_key = await self._get_channel_key(channel_id)
            encrypted_messages = []
            for msg in messages:
                msg_data = {
                    "id": msg["id"],
                    "sender": msg["sender"],
                    "created_at": msg["created_at"],
                    "read_by_user_at": msg.get("read_by_user_at"),
                    "read_by_claude_at": msg.get("read_by_claude_at"),
                }
                if channel_key:
                    payload = json.dumps({
                        "content": msg["content"],
                        "attachments": json.loads(msg["attachments"]) if msg.get("attachments") else None,
                    })
                    ct, nonce = encrypt(channel_key, payload)
                    msg_data["encrypted_payload"] = ct
                    msg_data["nonce"] = nonce
                else:
                    msg_data["content"] = msg["content"]
                    msg_data["attachments"] = json.loads(msg["attachments"]) if msg.get("attachments") else None

                encrypted_messages.append(msg_data)

            await self._ws_fire_and_forget({
                "type": "history_response",
                "request_id": request_id,
                "channel_id": channel_id,
                "messages": encrypted_messages,
                "has_more": has_more,
            })
            log.info("Sent %d history messages for channel %s", len(encrypted_messages), channel_id)

        except Exception as e:
            log.warning("History request failed for channel %s: %s", channel_id, e)

    async def _store_event_locally(self, event: dict) -> None:
        """Store incoming notification events in local SQLite.

        Handles both plaintext and E2E encrypted messages. If encrypted,
        decrypts before storing locally and patches the event dict with
        plaintext for forwarding to workers.
        """
        try:
            event_type = event.get("event_type")

            # User messages (may be encrypted from browser)
            if event_type == "aichat:message" and event.get("sender") == "user":
                content = event.get("content", "")
                attachments = event.get("attachments")

                # Decrypt if encrypted
                if event.get("encrypted_payload") and event.get("nonce"):
                    channel_id = event.get("channel_id", "")
                    plaintext = await self._decrypt_content(
                        channel_id, event["encrypted_payload"], event["nonce"]
                    )
                    if plaintext:
                        try:
                            payload = json.loads(plaintext)
                            content = payload.get("content", content)
                            attachments = payload.get("attachments", attachments)
                        except (json.JSONDecodeError, TypeError):
                            content = plaintext
                        # Patch event for worker forwarding
                        event["content"] = content
                        event["attachments"] = attachments

                await self.local_db.save_message(
                    channel_id=event["channel_id"],
                    sender="user",
                    content=content,
                    message_id=event.get("message_id"),
                    attachments=attachments if attachments else None,
                )

            # Event messages (plan mode etc)
            elif event_type == "aichat:message" and event.get("sender") == "event":
                await self.local_db.save_message(
                    channel_id=event["channel_id"],
                    sender="event",
                    content=event.get("content", ""),
                    message_id=event.get("message_id"),
                )

            # Mark-read from user side
            elif event_type == "aichat:read":
                message_ids = event.get("message_ids", [])
                if message_ids:
                    await self.local_db.mark_read_by_user(message_ids)

        except Exception as e:
            log.warning("Failed to store event locally: %s", e)

    # ------------------------------------------------------------------
    # E2E encryption helpers
    # ------------------------------------------------------------------

    async def _get_channel_key(self, channel_id: str) -> str | None:
        """Get the plaintext channel encryption key from local DB."""
        channel = await self.local_db.get_channel(channel_id)
        if channel and channel.get("channel_key_b64"):
            return channel["channel_key_b64"]
        return None

    async def _encrypt_content(self, channel_id: str, content: str) -> tuple[str, str] | None:
        """Encrypt content with the channel key. Returns (ciphertext_b64, nonce_b64) or None."""
        key = await self._get_channel_key(channel_id)
        if not key:
            return None
        return encrypt(key, content)

    async def _decrypt_content(self, channel_id: str, ciphertext_b64: str, nonce_b64: str) -> str | None:
        """Decrypt content with the channel key. Returns plaintext or None."""
        key = await self._get_channel_key(channel_id)
        if not key:
            return None
        try:
            return decrypt(key, ciphertext_b64, nonce_b64)
        except Exception as e:
            log.warning("Failed to decrypt content for channel %s: %s", channel_id, e)
            return None

    # ------------------------------------------------------------------
    # IPC: worker request proxying
    # ------------------------------------------------------------------

    async def _handle_worker_request(self, channel_id: str, msg: dict) -> dict:
        """Handle a request from a worker, proxying it to the server via WebSocket."""
        msg_type = msg.get("type")

        if msg_type == MSG_SEND_MESSAGE:
            content = msg["content"]
            ws_msg = {
                "type": "send_message",
                "channel_id": channel_id,
                "content": content,  # Plaintext fallback
            }

            # E2E: encrypt content if channel key available
            encrypted = await self._encrypt_content(channel_id, content)
            if encrypted:
                ct, nonce = encrypted
                ws_msg["encrypted_payload"] = ct
                ws_msg["nonce"] = nonce
                ws_msg["content"] = ""  # Don't send plaintext when encrypted

            result = await self._ws_request(ws_msg)

            # Store plaintext locally (always)
            try:
                await self.local_db.save_message(
                    channel_id=channel_id,
                    sender="claude",
                    content=content,
                    message_id=result.get("id") or result.get("message_id"),
                )
            except Exception as e:
                log.warning("Failed to save message to local DB: %s", e)
            return result

        if msg_type == MSG_TOOL_STATUS:
            description = msg.get("description", "")
            ws_msg = {
                "type": "tool_status",
                "channel_id": channel_id,
                "status": msg.get("status", ""),
                "tool": msg.get("tool", ""),
                "description": description,  # Plaintext fallback
            }

            # E2E: encrypt description if channel key available
            if description:
                encrypted = await self._encrypt_content(channel_id, description)
                if encrypted:
                    ct, nonce = encrypted
                    ws_msg["encrypted_description"] = ct
                    ws_msg["description_nonce"] = nonce
                    ws_msg["description"] = ""  # Don't send plaintext

            await self._ws_fire_and_forget(ws_msg)

            # Store plaintext locally
            try:
                tool_content = json.dumps({
                    "status": msg.get("status", ""),
                    "tool": msg.get("tool", ""),
                    "description": description,
                })
                await self.local_db.save_message(
                    channel_id=channel_id,
                    sender="tools",
                    content=tool_content,
                )
            except Exception as e:
                log.warning("Failed to save tool status to local DB: %s", e)
            return {}

        if msg_type == MSG_MARK_READ:
            result = await self._ws_request({
                "type": "mark_read",
                "channel_id": channel_id,
                "message_ids": msg.get("message_ids", []),
            })
            # Dual-write: mark read locally
            try:
                await self.local_db.mark_read_by_claude(msg.get("message_ids", []))
            except Exception as e:
                log.warning("Failed to mark read in local DB: %s", e)
            return result

        if msg_type == MSG_CREATE_INTERACTION:
            content = msg["content"]
            ws_msg = {
                "type": "create_interaction",
                "channel_id": channel_id,
                "interaction_type": msg["interaction_type"],
                "content": content,  # Plaintext fallback
                "options": msg.get("options"),
                "multi_select": msg.get("multi_select", False),
            }

            # E2E: encrypt interaction content
            encrypted = await self._encrypt_content(channel_id, json.dumps({
                "content": content,
                "options": msg.get("options"),
            }))
            if encrypted:
                ct, nonce = encrypted
                ws_msg["encrypted_payload"] = ct
                ws_msg["nonce"] = nonce
                ws_msg["content"] = ""
                ws_msg["options"] = None

            return await self._ws_request(ws_msg)

        if msg_type == MSG_SEND_EVENT:
            result = await self._ws_request({
                "type": "send_event",
                "channel_id": channel_id,
                "event_type": msg["event_type"],
            })
            # Dual-write: store event locally
            try:
                await self.local_db.save_message(
                    channel_id=channel_id,
                    sender="event",
                    content=msg["event_type"],
                    message_id=result.get("id") or result.get("message_id"),
                )
            except Exception as e:
                log.warning("Failed to save event to local DB: %s", e)
            return result

        if msg_type == MSG_REPORT_DIRECTORIES:
            result = await self._ws_request({
                "type": "report_directories",
                "channel_id": channel_id,
                "working_directory": msg["working_directory"],
                "additional_directories": msg.get("additional_directories"),
            })
            # Dual-write: update channel directories locally
            try:
                channel = await self.local_db.get_channel(channel_id)
                if channel:
                    await self.local_db.save_channel(
                        channel_id=channel_id,
                        name=channel["name"],
                        working_directory=msg["working_directory"],
                        additional_directories=msg.get("additional_directories"),
                    )
            except Exception as e:
                log.warning("Failed to update directories in local DB: %s", e)
            return result

        if msg_type == MSG_DOWNLOAD_ATTACHMENT:
            # Attachment downloads stay as HTTP (binary data not suited for WS JSON)
            api = self._get_download_client(channel_id)
            path = await api.download_attachment(msg["url"])
            return {"path": path}

        log.warning("IPC: unknown request type from worker: %s", msg_type)
        raise ValueError(f"Unknown request type: {msg_type}")

    def _get_download_client(self, channel_id: str) -> AiChatAPI:
        """Get a minimal AiChatAPI client for binary downloads."""
        return AiChatAPI(
            base_url=self.base_url,
            device_key=self.private_key_b64,
            device_id=self.device_id,
            channel_id=channel_id,
        )

    # ------------------------------------------------------------------
    # IPC server lifecycle
    # ------------------------------------------------------------------

    async def start_ipc(self) -> None:
        """Start the IPC server."""
        self.ipc = IPCServer(
            socket_path=self.ipc_socket_path,
            on_request=self._handle_worker_request,
        )
        await self.ipc.start()

    async def stop_ipc(self) -> None:
        """Stop the IPC server."""
        if self.ipc:
            await self.ipc.stop()

    async def _forward_event_to_worker(self, event: dict) -> None:
        """Forward a notification event to the appropriate worker via IPC."""
        if not self.ipc:
            return

        for channel_id in self.ipc.connected_channels:
            ipc_msg = _parse_worker_event(event, channel_id)
            if ipc_msg:
                sent = await self.ipc.send_to_worker(channel_id, ipc_msg)
                if sent:
                    log.debug("Forwarded %s event to worker %s", ipc_msg["type"], channel_id)

    # ------------------------------------------------------------------
    # Worker management
    # ------------------------------------------------------------------

    def _save_worker_pids(self) -> None:
        """Persist current worker PIDs to device config."""
        try:
            config = load_device_config()
            if not config:
                return
            config.workers = {
                cid: {
                    "pid": w.proc.pid,
                    "started_at": w.started_at,
                    "working_directory": w.working_directory,
                }
                for cid, w in self.workers.items()
                if w.proc.pid is not None and isinstance(w.proc.pid, int)
            }
            save_device_config(config)
        except Exception as e:
            log.warning("Failed to save worker PIDs: %s", e)

    def _kill_stale_pid(self, channel_id: str, pid: int) -> None:
        """Kill a stale worker process from a previous run."""
        try:
            os.kill(pid, 0)  # Check if alive
            log.info("Killing stale worker for channel %s (pid=%s)", channel_id, pid)
            os.kill(pid, 15)  # SIGTERM
        except OSError:
            pass  # Already dead

    async def start_worker(
        self, channel_id: str, channel_token: str = "", working_directory: str = ""
    ) -> None:
        """Launch a worker subprocess for the given channel.

        Workers communicate with the manager via IPC. The device auth env vars
        are still set for any direct CLI tool usage, but the agent process itself
        uses the IPC socket for all server communication.
        """
        # Stop existing worker for this channel if any
        if channel_id in self.workers:
            await self.stop_worker(channel_id)

        env = {**os.environ}
        env.pop("CLAUDECODE", None)  # Prevent nested Claude Code detection
        env.pop("AICHAT_PRIVATE_KEY", None)  # Don't leak parent env

        # Set device auth env vars so CLI tools (aichat-send etc) route correctly
        env["AICHAT_DEVICE_KEY"] = self.private_key_b64
        env["AICHAT_DEVICE_ID"] = self.device_id
        env["AICHAT_CHANNEL_ID"] = channel_id

        # Use absolute path for agent script since cwd may differ
        agent_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "aichat_agent.py"
        )

        # Determine working directory: use specified dir if valid, else manager dir
        manager_dir = os.path.dirname(os.path.abspath(__file__))
        cwd = working_directory if working_directory and os.path.isdir(working_directory) else manager_dir

        cmd = [
            sys.executable, agent_script,
            "--device-key", self.private_key_b64,
            "--device-id", self.device_id,
            "--channel-id", channel_id,
            "--base-url", self.base_url,
            "--ipc-socket", self.ipc_socket_path,
        ]
        if working_directory:
            cmd.extend(["--working-directory", working_directory])

        proc = await asyncio.create_subprocess_exec(*cmd, env=env, cwd=cwd)
        self.workers[channel_id] = WorkerProcess(
            proc=proc,
            channel_id=channel_id,
            channel_token=channel_token,
            working_directory=working_directory,
        )
        log.info(
            "Started worker for channel %s (pid=%s, cwd=%s)",
            channel_id, proc.pid, cwd,
        )
        self._save_worker_pids()

    async def stop_worker(self, channel_id: str) -> None:
        """Gracefully stop a worker."""
        worker = self.workers.pop(channel_id, None)
        if not worker:
            return
        log.info("Stopping worker for channel %s", channel_id)
        try:
            worker.proc.terminate()
        except ProcessLookupError:
            self._save_worker_pids()
            return
        try:
            await asyncio.wait_for(worker.proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            log.warning("Worker %s did not exit in time, killing", channel_id)
            worker.proc.kill()
            await worker.proc.wait()
        self._save_worker_pids()

    async def handle_command(self, cmd: dict) -> None:
        """Handle a parsed device command."""
        command = cmd["command"]
        payload = cmd.get("payload", {})

        if command == "worker:start":
            await self.start_worker(
                payload["channel_id"],
                payload.get("channel_token", ""),
                payload.get("working_directory", ""),
            )
        elif command == "worker:stop":
            await self.stop_worker(payload["channel_id"])
        elif command == "worker:restart":
            channel_id = payload["channel_id"]
            token = self.workers.get(channel_id, None)
            channel_token = token.channel_token if token else payload.get("channel_token", "")
            working_directory = payload.get("working_directory", "")
            # Fall back to previously known working directory
            if not working_directory and token:
                working_directory = token.working_directory
            await self.start_worker(channel_id, channel_token, working_directory)
        elif command == "device:ping":
            log.info("Ping received, reporting status")
            await self.report_status()
        elif command == "device:update":
            log.info("Device update: %s", payload)
        else:
            log.warning("Unknown command: %s", command)

    # ------------------------------------------------------------------
    # Status & sync
    # ------------------------------------------------------------------

    def _get_worker_memory_mb(self, pid: int) -> float | None:
        """Get RSS memory usage for a worker process in MB."""
        try:
            if sys.platform == "darwin":
                import subprocess
                result = subprocess.run(
                    ["ps", "-o", "rss=", "-p", str(pid)],
                    capture_output=True, text=True, timeout=2,
                )
                if result.returncode == 0 and result.stdout.strip():
                    return int(result.stdout.strip()) / 1024  # KB → MB
            else:
                statm = f"/proc/{pid}/statm"
                with open(statm) as f:
                    pages = int(f.read().split()[1])  # RSS in pages
                    import os as _os
                    return pages * _os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
        except Exception:
            pass
        return None

    def get_status(self) -> dict:
        """Get current device status with worker health info."""
        workers = []
        for channel_id, worker in self.workers.items():
            status = "running" if worker.proc.returncode is None else "crashed"
            info = {
                "channel_id": channel_id,
                "status": status,
                "uptime": int(time.time() - worker.started_at),
            }
            if status == "running" and worker.proc.pid:
                mem = self._get_worker_memory_mb(worker.proc.pid)
                if mem is not None:
                    info["memory_mb"] = round(mem, 1)
            workers.append(info)
        return {
            "status": "online",
            "device_id": self.device_id,
            "workers": workers,
        }

    async def sync_channels(self) -> None:
        """Query server for assigned channels and reconcile with running workers."""
        try:
            data = await self._ws_request({"type": "list_channels"})
        except Exception as e:
            log.warning("Failed to fetch device channels: %s", e)
            return

        server_channels = {ch["id"]: ch for ch in data.get("channels", [])}

        # Dual-write: sync channel metadata to local DB and generate encryption keys
        for ch_id, ch_info in server_channels.items():
            try:
                existing = await self.local_db.get_channel(ch_id)
                channel_key_b64 = existing["channel_key_b64"] if existing and existing.get("channel_key_b64") else None

                # Generate channel key if missing
                if not channel_key_b64:
                    channel_key_b64 = generate_channel_key()
                    log.info("Generated encryption key for channel %s", ch_id)

                    # Upload encrypted channel key to server if we have a device master key
                    if self.device_master_key_b64:
                        await self._upload_encrypted_channel_key(ch_id, channel_key_b64)

                await self.local_db.save_channel(
                    channel_id=ch_id,
                    name=ch_info.get("name", ""),
                    device_id=self.device_id,
                    working_directory=ch_info.get("working_directory"),
                    channel_key_b64=channel_key_b64,
                )
            except Exception as e:
                log.warning("Failed to sync channel %s to local DB: %s", ch_id, e)
        server_channel_ids = set(server_channels.keys())
        local_channel_ids = set(self.workers.keys())

        # Check for stale PIDs from a previous run
        config = load_device_config()
        saved_pids = config.workers if config else {}

        # Kill stale PIDs for channels no longer assigned
        for channel_id, info in saved_pids.items():
            if channel_id not in server_channel_ids:
                pid = info.get("pid")
                if pid:
                    self._kill_stale_pid(channel_id, pid)

        # Stop live workers for channels no longer assigned
        for channel_id in local_channel_ids - server_channel_ids:
            log.info("Channel %s removed from device, stopping worker", channel_id)
            await self.stop_worker(channel_id)

        # Start workers for channels not yet running (unless stale PID is still alive)
        for channel_id in server_channel_ids - local_channel_ids:
            if channel_id in saved_pids:
                pid = saved_pids[channel_id].get("pid")
                if pid:
                    try:
                        os.kill(pid, 0)  # Check if alive
                        log.info("Channel %s has live worker from previous run (pid=%s), keeping it", channel_id, pid)
                        continue
                    except OSError:
                        pass  # Dead, start a new one
            ch_info = server_channels[channel_id]
            working_directory = ch_info.get("working_directory", "")
            log.info("Channel %s assigned to device, starting worker (cwd=%s)", channel_id, working_directory or "<default>")
            await self.start_worker(channel_id, working_directory=working_directory)

    async def _upload_encrypted_channel_key(
        self, channel_id: str, channel_key_b64: str
    ) -> None:
        """Encrypt a channel key with the device master key and upload to server."""
        try:
            enc_key, nonce = encrypt_channel_key(
                self.device_master_key_b64, channel_key_b64
            )
            await self._ws_request({
                "type": "register_channel_key",
                "channel_id": channel_id,
                "encrypted_channel_key": enc_key,
                "key_nonce": nonce,
            })
            log.info("Uploaded encrypted key for channel %s", channel_id)
        except Exception as e:
            log.warning("Failed to upload channel key for %s: %s", channel_id, e)

    async def report_status(self) -> None:
        """Report device status to the server via WebSocket."""
        status = self.get_status()
        try:
            await self._ws_request({
                "type": "report_status",
                **status,
            })
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

    # ------------------------------------------------------------------
    # Worker monitoring
    # ------------------------------------------------------------------

    async def _check_workers(self) -> None:
        """Check for crashed workers and restart them."""
        for channel_id, worker in list(self.workers.items()):
            if worker.proc.returncode is not None:
                log.warning("Worker %s crashed (rc=%s), restarting", channel_id, worker.proc.returncode)
                await self.start_worker(channel_id, worker.channel_token, worker.working_directory)

    async def monitor_workers(self) -> None:
        """Continuously monitor workers for crashes."""
        while True:
            await self._check_workers()
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def shutdown(self) -> None:
        """Gracefully stop all workers, IPC server, and local database."""
        log.info("Shutting down, stopping %d workers", len(self.workers))
        for channel_id in list(self.workers.keys()):
            await self.stop_worker(channel_id)
        await self.stop_ipc()
        await self.local_db.close()


async def device_auth_flow(base_url: str) -> DeviceConfig:
    """Run the interactive device auth flow with X25519 key exchange."""
    from aichat_crypto import derive_device_master_key_b64, generate_x25519_keypair

    device_name = platform.node() or "Unknown Device"
    keypair = generate_device_keypair()
    x25519_kp = generate_x25519_keypair()

    log.info("Registering device '%s'...", device_name)
    result = await register_device(
        base_url=base_url,
        public_key_b64=keypair["public_key_b64"],
        device_name=device_name,
        x25519_public_b64=x25519_kp["public_key_b64"],
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

            # Derive device master key from ECDH if browser provided its X25519 public key
            device_master_key_b64 = ""
            browser_x25519_public = status.get("browser_x25519_public", "")
            if browser_x25519_public:
                device_master_key_b64 = derive_device_master_key_b64(
                    x25519_kp["private_key_b64"], browser_x25519_public
                )
                log.info("E2E key exchange completed — device master key derived")
            else:
                log.warning("Browser did not provide X25519 public key — E2E encryption unavailable")

            config = DeviceConfig(
                device_id=status["device_id"],
                device_name=device_name,
                private_key_b64=keypair["private_key_b64"],
                public_key_b64=keypair["public_key_b64"],
                base_url=base_url,
                x25519_private_b64=x25519_kp["private_key_b64"],
                x25519_public_b64=x25519_kp["public_key_b64"],
                device_master_key_b64=device_master_key_b64,
            )
            save_device_config(config)
            return config
        elif status["status"] == "denied":
            raise SystemExit("Device authorization denied.")
        # Still pending
        await asyncio.sleep(2)


async def run_manager() -> None:
    """Main manager entry point."""
    from aichat_crypto import generate_x25519_keypair

    base_url = os.environ.get("AICHAT_URL", "https://aichat.zech.sh")

    # Load or create device config
    config = load_device_config()
    if not config:
        config = await device_auth_flow(base_url)

    # Migrate existing device: generate X25519 keypair if missing
    if not config.x25519_private_b64 or not config.x25519_public_b64:
        log.info("Generating X25519 keypair for existing device (E2E migration)...")
        x25519_kp = generate_x25519_keypair()
        config.x25519_private_b64 = x25519_kp["private_key_b64"]
        config.x25519_public_b64 = x25519_kp["public_key_b64"]
        # Clear any stale master key — it'll be derived on first rekey
        config.device_master_key_b64 = ""
        save_device_config(config)
        log.info("X25519 keypair saved — E2E will activate on next browser key exchange")

    log.info("Device: %s (%s)", config.device_name, config.device_id)

    manager = DeviceManager(
        device_id=config.device_id,
        private_key_b64=config.private_key_b64,
        base_url=config.base_url,
        device_master_key_b64=config.device_master_key_b64,
        x25519_private_b64=config.x25519_private_b64,
    )

    # Open local database and start IPC server before connecting WebSocket
    await manager.local_db.open()
    await manager.start_ipc()

    try:
        # WebSocket connection loop and worker monitor run concurrently.
        # connect_websocket handles initial sync (report_status + sync_channels)
        # on each (re)connect.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(manager.connect_websocket())
            tg.create_task(manager.monitor_workers())
    except KeyboardInterrupt:
        pass
    finally:
        await manager.shutdown()


def main() -> None:
    asyncio.run(run_manager())


if __name__ == "__main__":
    main()
