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
import signal
import time
import uuid
from dataclasses import dataclass

import websockets

from aichat_api import AiChatAPI
from aichat_crypto import KeyExchange, encrypt as crypto_encrypt, decrypt as crypto_decrypt
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

    proc: asyncio.subprocess.Process | None
    channel_id: str
    channel_token: str
    started_at: float = 0.0
    working_directory: str = ""
    agent_type: str = "claude"
    adopted: bool = False
    adopted_pid: int | None = None

    def __post_init__(self):
        if not self.started_at:
            self.started_at = time.time()

    @property
    def pid(self) -> int | None:
        if self.adopted:
            return self.adopted_pid
        return self.proc.pid if self.proc else None

    def is_alive(self) -> bool:
        """Check if the worker process is still running."""
        if self.adopted:
            if self.adopted_pid is None:
                return False
            try:
                os.kill(self.adopted_pid, 0)
                return True
            except OSError:
                return False
        if self.proc is None:
            return False
        return self.proc.returncode is None


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
                 key_exchange: KeyExchange | None = None,
                 encryption_key_b64: str = "",
                 default_agent_type: str = "claude"):
        self.device_id = device_id
        self.private_key_b64 = private_key_b64
        self.base_url = base_url
        self.key_exchange = key_exchange  # Only used for ECDH transport in rekey
        self.encryption_key_b64 = encryption_key_b64  # Canonical key for all message encryption
        self.default_agent_type = default_agent_type
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
        if not self.key_exchange:
            return
        try:
            await self._ws_request({
                "type": "update_device_x25519",
                "x25519_public": self.key_exchange.public_key_b64,
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
            log.debug("Dropping WS fire-and-forget message while disconnected: %s", msg.get("type"))
            return
        try:
            async with self._ws_lock:
                await self._ws.send(json.dumps(msg))
        except Exception as e:
            log.debug("WS fire-and-forget send failed for %s: %s", msg.get("type"), e)

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

        # Direct user content relay (encrypted content pushed from server, bypasses notifications)
        if event_type == "aichat:user-content":
            await self._handle_user_content_relay(event)
            return

        # Track timestamp for potential replay
        if "timestamp" in event:
            self.last_event_ts = event["timestamp"]

        # Dual-write: store incoming messages locally
        await self._store_event_locally(event)

        # Forward worker-relevant events via IPC
        # Skip metadata-only user messages — the user-content relay provides the real content
        if self.ipc:
            if (
                event_type == "aichat:message"
                and event.get("sender") == "user"
                and not event.get("content")
            ):
                log.debug("Skipping metadata-only user message, waiting for user-content relay")
            else:
                await self._forward_event_to_worker(event)

    async def _handle_rekey_request(self, event: dict) -> None:
        """Handle a re-key request from a browser session.

        Browser sends its X25519 public key. We derive a transport key via ECDH,
        then wrap the canonical encryption key with it for delivery. The encryption
        key is always a random key generated on first manager start — ECDH is purely
        a transport mechanism.
        """
        browser_x25519_public = event.get("browser_x25519_public", "")
        request_id = event.get("request_id", "")

        if not browser_x25519_public or not self.key_exchange:
            log.warning("Re-key request missing browser key or device X25519 key")
            return

        if not self.encryption_key_b64:
            log.warning("Re-key request but no encryption key available")
            return

        try:
            transport_key = self.key_exchange.complete_exchange(browser_x25519_public)

            # Always wrap the encryption key with this browser's transport key
            ct, nonce = crypto_encrypt(transport_key, self.encryption_key_b64)
            response = {
                "type": "rekey_response",
                "request_id": request_id,
                "encrypted_key": ct,
                "nonce": nonce,
            }
            await self._ws_fire_and_forget(response)
            log.info("Re-key complete — delivered encryption key to browser")

        except Exception as e:
            log.warning("Re-key failed: %s", e)

    async def _handle_history_request(self, event: dict) -> None:
        """Handle a history request from the browser, proxied via server.

        Query local SQLite and send encrypted messages back.
        """
        channel_id = event.get("channel_id", "")
        request_id = event.get("request_id", "")
        before_id = event.get("before")
        limit = min(event.get("limit", 100), 500)
        message_ids = event.get("message_ids")

        if not channel_id or not request_id:
            return

        try:
            # If specific message IDs requested, fetch those directly
            if message_ids and isinstance(message_ids, list):
                messages = await self.local_db.get_messages_by_ids(
                    channel_id, message_ids[:200]
                )
                has_more = False
            else:
                messages = await self.local_db.get_messages(
                    channel_id, limit=limit + 1, before_id=before_id
                )
                has_more = len(messages) > limit
                messages = messages[:limit]

            # Encrypt each message for the browser
            encrypted_messages = []
            for msg in messages:
                msg_data = {
                    "id": msg["id"],
                    "sender": msg["sender"],
                    "created_at": msg["created_at"],
                    "read_by_user_at": msg.get("read_by_user_at"),
                    "read_by_claude_at": msg.get("read_by_claude_at"),
                }
                payload = json.dumps({
                    "content": msg["content"],
                    "attachments": json.loads(msg["attachments"]) if msg.get("attachments") else None,
                })
                if self.encryption_key_b64:
                    ct, nonce = crypto_encrypt(self.encryption_key_b64, payload)
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

    async def _handle_user_content_relay(self, event: dict) -> None:
        """Handle encrypted user message content pushed directly via WebSocket.

        The server pushes encrypted content here (bypassing notifications) so
        ciphertext is never persisted. We decrypt, store plaintext locally,
        and forward to the worker.
        """
        channel_id = event.get("channel_id", "")
        encrypted_payload = event.get("encrypted_payload", "")
        nonce = event.get("nonce", "")
        message_id = event.get("message_id", "")

        if not channel_id or not encrypted_payload or not nonce:
            return

        content = ""
        attachments: list = []
        if self.encryption_key_b64:
            try:
                plain = crypto_decrypt(self.encryption_key_b64, encrypted_payload, nonce)
                if plain:
                    payload = json.loads(plain)
                    content = payload.get("content", "")
                    attachments = payload.get("attachments") or []
            except Exception as e:
                log.warning("Failed to decrypt user content relay: %s", e)

        # Store plaintext locally
        try:
            await self.local_db.save_message(
                channel_id=channel_id,
                sender="user",
                content=content,
                message_id=message_id,
                attachments=attachments if attachments else None,
            )
        except Exception as e:
            log.warning("Failed to save user content to local DB: %s", e)

        # Forward to worker as a plaintext event
        if self.ipc:
            await self._forward_event_to_worker({
                "event_type": "aichat:message",
                "sender": "user",
                "content": content,
                "message_id": message_id,
                "channel_id": channel_id,
                "attachments": attachments,
            })

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
                if event.get("encrypted_payload") and event.get("nonce") and self.encryption_key_b64:
                    try:
                        plaintext = crypto_decrypt(
                            self.encryption_key_b64, event["encrypted_payload"], event["nonce"]
                        )
                    except Exception:
                        plaintext = None
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
    # IPC: worker request proxying
    # ------------------------------------------------------------------

    async def _handle_worker_request(self, channel_id: str, msg: dict) -> dict:
        """Handle a request from a worker, proxying it to the server via WebSocket."""
        msg_type = msg.get("type")

        if msg_type == MSG_SEND_MESSAGE:
            content = msg["content"]
            worker = self.workers.get(channel_id)
            sender = worker.agent_type if worker else "claude"
            ws_msg = {
                "type": "send_message",
                "channel_id": channel_id,
                "content": content,  # Plaintext fallback
                "sender": sender,
            }

            # E2E: encrypt content if encryption key is available
            attachments = msg.get("attachments")
            encrypted = crypto_encrypt(
                self.encryption_key_b64,
                json.dumps({"content": content, "attachments": attachments or []})
            ) if self.encryption_key_b64 else None
            if encrypted:
                ct, nonce = encrypted
                ws_msg["encrypted_payload"] = ct
                ws_msg["nonce"] = nonce
                ws_msg["content"] = ""  # Don't send plaintext when encrypted

            result = await self._ws_request(ws_msg)

            # Store plaintext locally (always)
            message_id = result.get("id") or result.get("message_id")
            try:
                await self.local_db.save_message(
                    channel_id=channel_id,
                    sender=sender,
                    content=content,
                    message_id=message_id,
                )
            except Exception as e:
                log.warning("Failed to save message to local DB: %s", e)

            # E2E: send encrypted content via ephemeral relay (separate from notification)
            if encrypted:
                ct, nonce = encrypted
                await self._ws_fire_and_forget({
                    "type": "relay_content",
                    "channel_id": channel_id,
                    "content_type": "message",
                    "message_id": message_id,
                    "encrypted_payload": ct,
                    "nonce": nonce,
                })

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

            # E2E: encrypt description — send via ephemeral relay, not in notification
            encrypted = None
            if description and self.encryption_key_b64:
                encrypted = crypto_encrypt(self.encryption_key_b64, description)
                if encrypted:
                    ws_msg["description"] = ""  # Don't send plaintext

            await self._ws_fire_and_forget(ws_msg)

            # Send encrypted content via ephemeral relay
            if encrypted:
                ct, nonce = encrypted
                await self._ws_fire_and_forget({
                    "type": "relay_content",
                    "channel_id": channel_id,
                    "content_type": "tool",
                    "encrypted_payload": ct,
                    "nonce": nonce,
                })

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

            # E2E: encrypt interaction content — send via ephemeral relay
            encrypted = crypto_encrypt(self.encryption_key_b64, json.dumps({
                "content": content,
                "options": msg.get("options"),
            })) if self.encryption_key_b64 else None
            if encrypted:
                ct, nonce = encrypted
                ws_msg["content"] = ""
                ws_msg["options"] = None

            result = await self._ws_request(ws_msg)

            # Send encrypted content via ephemeral relay
            if encrypted:
                ct, nonce = encrypted
                await self._ws_fire_and_forget({
                    "type": "relay_content",
                    "channel_id": channel_id,
                    "content_type": "interaction",
                    "interaction_id": result.get("interaction_id", ""),
                    "encrypted_payload": ct,
                    "nonce": nonce,
                })

            return result

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
                    "pid": w.pid,
                    "started_at": w.started_at,
                    "working_directory": w.working_directory,
                    "agent_type": w.agent_type,
                }
                for cid, w in self.workers.items()
                if w.pid is not None and isinstance(w.pid, int)
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
        self, channel_id: str, channel_token: str = "", working_directory: str = "",
        agent_type: str = "",
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

        # Select agent script based on type
        effective_agent_type = agent_type or self.default_agent_type
        script_dir = os.path.dirname(os.path.abspath(__file__))
        if effective_agent_type == "codex":
            agent_script = os.path.join(script_dir, "codex_agent.py")
        else:
            agent_script = os.path.join(script_dir, "aichat_agent.py")

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
            agent_type=effective_agent_type,
        )
        log.info(
            "Started %s worker for channel %s (pid=%s, cwd=%s)",
            effective_agent_type, channel_id, proc.pid, cwd,
        )
        self._save_worker_pids()

        # Persist agent_type to local DB (survives device.json overwrites)
        try:
            channel = await self.local_db.get_channel(channel_id)
            if channel:
                await self.local_db.save_channel(
                    channel_id, channel["name"], agent_type=effective_agent_type,
                )
        except Exception as e:
            log.warning("Failed to save agent_type to local DB: %s", e)

    async def stop_worker(self, channel_id: str) -> None:
        """Gracefully stop a worker."""
        worker = self.workers.pop(channel_id, None)
        if not worker:
            return
        log.info("Stopping worker for channel %s (adopted=%s)", channel_id, worker.adopted)
        if worker.adopted:
            # Adopted worker — no proc object, signal directly
            if worker.adopted_pid:
                try:
                    os.kill(worker.adopted_pid, signal.SIGTERM)
                except OSError:
                    pass
            self._save_worker_pids()
            return
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

    async def rename_device(self, name: str) -> bool:
        """Rename this device via WebSocket."""
        try:
            data = await self._ws_request({
                "type": "rename_device",
                "name": name,
            })
            device = data["device"]
            log.info("Device renamed to %s", device["name"])
            # Update local config
            config = load_device_config()
            if config:
                config.device_name = device["name"]
                save_device_config(config)
            return True
        except Exception as e:
            log.error("Failed to rename device: %s", e)
            return False

    async def delete_device(self) -> bool:
        """Delete this device via WebSocket. Stops all workers first."""
        try:
            # Stop all workers
            for channel_id in list(self.workers):
                await self.stop_worker(channel_id)
            await self._ws_request({"type": "delete_device"})
            log.info("Device deleted from server")
            # Remove local config
            import pathlib
            config_path = pathlib.Path.home() / ".config" / "aichat" / "device.json"
            if config_path.exists():
                config_path.unlink()
                log.info("Local device config removed")
            return True
        except Exception as e:
            log.error("Failed to delete device: %s", e)
            return False

    async def create_channel(self, name: str, working_directory: str = "", agent_type: str = "") -> str | None:
        """Create a new channel via WebSocket and start its worker."""
        try:
            data = await self._ws_request({
                "type": "create_channel",
                "name": name,
                "working_directory": working_directory,
            })
            channel = data["channel"]
            channel_id = channel["id"]
            wd = channel.get("working_directory", working_directory)
            log.info("Created channel %s (%s)", channel["name"], channel_id)

            # Sync to local DB
            await self.local_db.save_channel(
                channel_id=channel_id,
                name=channel["name"],
                device_id=self.device_id,
                working_directory=wd,
            )

            await self.start_worker(channel_id, working_directory=wd, agent_type=agent_type)
            return channel_id
        except Exception as e:
            log.error("Failed to create channel: %s", e)
            return None

    async def handle_command(self, cmd: dict) -> None:
        """Handle a parsed device command."""
        command = cmd["command"]
        payload = cmd.get("payload", {})

        if command == "worker:start":
            await self.start_worker(
                payload["channel_id"],
                payload.get("channel_token", ""),
                payload.get("working_directory", ""),
                agent_type=payload.get("agent_type", ""),
            )
        elif command == "worker:stop":
            await self.stop_worker(payload["channel_id"])
        elif command == "worker:restart":
            channel_id = payload["channel_id"]
            token = self.workers.get(channel_id, None)
            channel_token = token.channel_token if token else payload.get("channel_token", "")
            working_directory = payload.get("working_directory", "")
            prev_agent_type = token.agent_type if token else ""
            # Fall back to previously known working directory
            if not working_directory and token:
                working_directory = token.working_directory
            await self.start_worker(
                channel_id, channel_token, working_directory,
                agent_type=payload.get("agent_type", prev_agent_type),
            )
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
            status = "running" if worker.is_alive() else "crashed"
            pid = worker.pid
            info = {
                "channel_id": channel_id,
                "status": status,
                "uptime": int(time.time() - worker.started_at),
            }
            if status == "running" and pid:
                mem = self._get_worker_memory_mb(pid)
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

        # Sync channel metadata to local DB
        for ch_id, ch_info in server_channels.items():
            try:
                await self.local_db.save_channel(
                    channel_id=ch_id,
                    name=ch_info.get("name", ""),
                    device_id=self.device_id,
                    working_directory=ch_info.get("working_directory"),
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

        # Start workers for channels not yet running (adopt live PIDs from previous run)
        for channel_id in server_channel_ids - local_channel_ids:
            # Resolve agent_type: local DB (authoritative) > device.json > default
            local_channel = await self.local_db.get_channel(channel_id)
            db_agent_type = (local_channel or {}).get("agent_type", "")
            pid_agent_type = saved_pids.get(channel_id, {}).get("agent_type", "")
            resolved_agent_type = db_agent_type or pid_agent_type or ""

            if channel_id in saved_pids:
                pid = saved_pids[channel_id].get("pid")
                if pid:
                    try:
                        os.kill(pid, 0)  # Check if alive
                        saved_wd = saved_pids[channel_id].get("working_directory", "")
                        saved_started = saved_pids[channel_id].get("started_at", 0.0)
                        log.info("Adopting live worker for channel %s (pid=%s, agent=%s)", channel_id, pid, resolved_agent_type or self.default_agent_type)
                        self.workers[channel_id] = WorkerProcess(
                            proc=None,
                            channel_id=channel_id,
                            channel_token="",
                            started_at=saved_started,
                            working_directory=saved_wd,
                            adopted=True,
                            adopted_pid=pid,
                            agent_type=resolved_agent_type or self.default_agent_type,
                        )
                        continue
                    except OSError:
                        pass  # Dead, start a new one
            ch_info = server_channels[channel_id]
            working_directory = ch_info.get("working_directory", "")
            log.info("Channel %s assigned to device, starting worker (cwd=%s, agent=%s)", channel_id, working_directory or "<default>", resolved_agent_type or self.default_agent_type)
            await self.start_worker(channel_id, working_directory=working_directory, agent_type=resolved_agent_type)

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
            if not worker.is_alive():
                if worker.adopted:
                    log.warning("Adopted worker %s (pid=%s) died, restarting", channel_id, worker.adopted_pid)
                else:
                    log.warning("Worker %s crashed (rc=%s), restarting", channel_id, worker.proc.returncode)
                await self.start_worker(channel_id, worker.channel_token, worker.working_directory, agent_type=worker.agent_type)

    async def monitor_workers(self) -> None:
        """Continuously monitor workers for crashes."""
        while True:
            await self._check_workers()
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def shutdown(self, force: bool = False) -> None:
        """Shut down the manager. Workers are detached by default (surviving restart).

        Args:
            force: If True, terminate all workers before exiting.
        """
        if force:
            log.info("Shutting down (force), stopping %d workers", len(self.workers))
            for channel_id in list(self.workers.keys()):
                await self.stop_worker(channel_id)
        else:
            log.info("Shutting down, detaching %d workers (they will keep running)", len(self.workers))
            self._save_worker_pids()
        await self.stop_ipc()
        await self.local_db.close()


async def device_auth_flow(base_url: str) -> DeviceConfig:
    """Run the interactive device auth flow with X25519 key exchange."""
    from aichat_crypto import generate_x25519_keypair

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

            # Encryption key will be generated randomly on first manager start
            config = DeviceConfig(
                device_id=status["device_id"],
                device_name=device_name,
                private_key_b64=keypair["private_key_b64"],
                public_key_b64=keypair["public_key_b64"],
                base_url=base_url,
                x25519_private_b64=x25519_kp["private_key_b64"],
                x25519_public_b64=x25519_kp["public_key_b64"],
                encryption_key_b64="",
            )
            save_device_config(config)
            return config
        elif status["status"] == "denied":
            raise SystemExit("Device authorization denied.")
        # Still pending
        await asyncio.sleep(2)


async def run_manager(
    force_stop: bool = False,
    create_channel_name: str | None = None,
    create_channel_wd: str = "",
    default_agent_type: str = "claude",
) -> None:
    """Main manager entry point."""
    from aichat_crypto import KeyExchange, generate_x25519_keypair

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
        config.encryption_key_b64 = ""
        save_device_config(config)
        log.info("X25519 keypair saved — E2E will activate on next browser key exchange")

    # Generate a random encryption key if none exists yet
    if not config.encryption_key_b64:
        import base64 as _b64
        config.encryption_key_b64 = _b64.b64encode(os.urandom(32)).decode()
        save_device_config(config)
        log.info("Generated new random encryption key")

    # Build KeyExchange for ECDH transport during rekey
    key_exchange: KeyExchange | None = None
    if config.x25519_private_b64 and config.x25519_public_b64:
        key_exchange = KeyExchange(config.x25519_private_b64, config.x25519_public_b64)

    log.info("Device: %s (%s)", config.device_name, config.device_id)

    manager = DeviceManager(
        device_id=config.device_id,
        private_key_b64=config.private_key_b64,
        base_url=config.base_url,
        key_exchange=key_exchange,
        encryption_key_b64=config.encryption_key_b64,
        default_agent_type=default_agent_type,
    )

    # Open local database and start IPC server before connecting WebSocket
    await manager.local_db.open()
    await manager.start_ipc()

    async def handle_stdin(mgr: DeviceManager) -> None:
        """Read commands from stdin (non-blocking)."""
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), sys.stdin
        )
        while True:
            try:
                line_bytes = await reader.readline()
                if not line_bytes:
                    return  # EOF
                line = line_bytes.decode().strip()
                if not line:
                    continue
                parts = line.split(None, 2)
                cmd = parts[0].lower()
                if cmd in ("new", "create"):
                    name = parts[1] if len(parts) > 1 else ""
                    wd = parts[2] if len(parts) > 2 else ""
                    # Check for --codex flag in remaining args
                    at = "codex" if any(p == "--codex" for p in parts) else ""
                    wd = wd if wd != "--codex" else ""
                    await mgr.create_channel(name, wd, agent_type=at)
                elif cmd == "rename":
                    name = " ".join(parts[1:]) if len(parts) > 1 else ""
                    if not name:
                        print("Usage: rename <new name>")
                    else:
                        await mgr.rename_device(name)
                elif cmd == "delete":
                    confirm = parts[1] if len(parts) > 1 else ""
                    if confirm != "confirm":
                        print("This will delete the device and all workers.")
                        print("Type 'delete confirm' to proceed.")
                    else:
                        if await mgr.delete_device():
                            print("Device deleted. Exiting.")
                            return
                elif cmd == "status":
                    status = mgr.get_status()
                    for w in status["workers"]:
                        mem = f" ({w['memory_mb']}MB)" if "memory_mb" in w else ""
                        print(f"  {w['channel_id']}  {w['status']}  uptime={w['uptime']}s{mem}")
                    if not status["workers"]:
                        print("  No workers running")
                elif cmd in ("help", "?"):
                    print("Commands:")
                    print("  new <name> [working_directory] [--codex]  — create a channel and start a worker")
                    print("  rename <name>                   — rename this device")
                    print("  delete confirm                  — delete this device and exit")
                    print("  status                          — show worker status")
                    print("  help                            — show this help")
                else:
                    print(f"Unknown command: {cmd}  (try 'help')")
            except Exception as e:
                log.warning("stdin handler error: %s", e)

    async def create_on_startup(mgr: DeviceManager, name: str, wd: str) -> None:
        """Wait for WebSocket connection, then create a channel."""
        # Wait until the WS is connected and initial sync is done
        while not mgr._ws:
            await asyncio.sleep(0.2)
        # Small delay to let initial sync complete
        await asyncio.sleep(1)
        await mgr.create_channel(name, wd)

    try:
        # WebSocket connection loop, worker monitor, and stdin handler run concurrently.
        # connect_websocket handles initial sync (report_status + sync_channels)
        # on each (re)connect.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(manager.connect_websocket())
            tg.create_task(manager.monitor_workers())
            tg.create_task(handle_stdin(manager))
            if create_channel_name:
                tg.create_task(create_on_startup(manager, create_channel_name, create_channel_wd))
    except KeyboardInterrupt:
        pass
    finally:
        await manager.shutdown(force=force_stop)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="AI.CHAT Device Manager")
    parser.add_argument(
        "--stop",
        action="store_true",
        help="Kill all workers on shutdown instead of detaching them",
    )
    parser.add_argument(
        "--new",
        metavar="NAME",
        help="Create a new channel with the given name on startup",
    )
    parser.add_argument(
        "--working-directory",
        metavar="DIR",
        default="",
        help="Working directory for the new channel (used with --new)",
    )
    parser.add_argument(
        "--agent-type",
        choices=["claude", "codex"],
        default="claude",
        help="Default agent type for workers (default: claude)",
    )
    args = parser.parse_args()
    asyncio.run(run_manager(
        force_stop=args.stop,
        create_channel_name=args.new,
        create_channel_wd=args.working_directory,
        default_agent_type=args.agent_type,
    ))


if __name__ == "__main__":
    main()
