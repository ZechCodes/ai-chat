"""IPC layer for manager ↔ worker communication over Unix domain sockets.

Uses NDJSON (newline-delimited JSON) for framing. The manager runs an IPCServer
that accepts connections from worker processes. Each worker connects via IPCClient
and identifies itself with a channel_id.

Message flow:
  Worker -> Manager: requests (send_message, tool_status, etc.) with a `rid` for correlation
  Manager -> Worker: responses (keyed by `rid`) and pushed events (messages, interactions)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Any, Callable, Awaitable

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Message type constants
# ---------------------------------------------------------------------------

# Worker -> Manager
MSG_REGISTER = "register"
MSG_SEND_MESSAGE = "send_message"
MSG_TOOL_STATUS = "tool_status"
MSG_MARK_READ = "mark_read"
MSG_CREATE_INTERACTION = "create_interaction"
MSG_SEND_EVENT = "send_event"
MSG_REPORT_DIRECTORIES = "report_directories"
MSG_DOWNLOAD_ATTACHMENT = "download_attachment"
MSG_GET_SESSION = "get_session"
MSG_GET_UNREAD = "get_unread"

# Manager -> Worker (responses)
MSG_RESPONSE = "response"

# Manager -> Worker (pushed events)
MSG_EVENT_MESSAGE = "message"
MSG_EVENT_PLAN = "plan_event"
MSG_EVENT_INTERACTION_RESPONSE = "interaction_response"


def _generate_rid() -> str:
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# IPCServer — used by the manager
# ---------------------------------------------------------------------------

class IPCServer:
    """Unix domain socket server that accepts worker connections."""

    def __init__(
        self,
        socket_path: str,
        on_request: Callable[[str, dict], Awaitable[dict]],
    ):
        """
        Args:
            socket_path: Path for the UDS (e.g. /tmp/aichat-{device_id}.sock)
            on_request: Async callback invoked for each worker request.
                        Receives (channel_id, message_dict) and should return a response dict.
        """
        self.socket_path = socket_path
        self.on_request = on_request
        # Primary worker connections (receive events)
        self._workers: dict[str, asyncio.StreamWriter] = {}  # channel_id -> writer
        # Additional connections (e.g. MCP server subprocesses) — can send requests but don't receive events
        self._aux_connections: dict[str, list[asyncio.StreamWriter]] = {}
        self._server: asyncio.Server | None = None

    @property
    def connected_channels(self) -> set[str]:
        return set(self._workers.keys())

    async def start(self) -> None:
        """Start listening on the Unix domain socket."""
        # Clean up stale socket file
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

        self._server = await asyncio.start_unix_server(
            self._handle_connection, path=self.socket_path
        )
        # Restrict socket permissions
        os.chmod(self.socket_path, 0o600)
        log.info("IPC server listening on %s", self.socket_path)

    async def stop(self) -> None:
        """Stop the server and close all connections."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        for writer in self._workers.values():
            writer.close()
        self._workers.clear()

        for writers in self._aux_connections.values():
            for writer in writers:
                writer.close()
        self._aux_connections.clear()

        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

    async def send_to_worker(self, channel_id: str, message: dict) -> bool:
        """Send a message to a specific worker. Returns False if not connected."""
        writer = self._workers.get(channel_id)
        if not writer:
            return False
        try:
            await _write_message(writer, message)
            return True
        except (ConnectionError, OSError) as e:
            log.warning("Failed to send to worker %s: %s", channel_id, e)
            self._workers.pop(channel_id, None)
            return False

    async def broadcast_to_workers(self, message: dict) -> None:
        """Send a message to all connected workers."""
        for channel_id in list(self._workers):
            await self.send_to_worker(channel_id, message)

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a single worker connection."""
        channel_id: str | None = None
        is_primary = False  # True if this connection is the primary worker (receives events)
        try:
            async for msg in _read_messages(reader):
                if msg.get("type") == MSG_REGISTER:
                    channel_id = msg["channel_id"]
                    if channel_id not in self._workers:
                        # First connection for this channel — primary worker
                        self._workers[channel_id] = writer
                        is_primary = True
                        log.info("IPC: worker registered for channel %s", channel_id)
                    else:
                        # Additional connection (e.g. MCP server subprocess)
                        self._aux_connections.setdefault(channel_id, []).append(writer)
                        log.info("IPC: auxiliary connection registered for channel %s", channel_id)
                    continue

                if not channel_id:
                    log.warning("IPC: received message before registration, ignoring")
                    continue

                # Forward to the request handler and send response
                rid = msg.get("rid")
                try:
                    result = await self.on_request(channel_id, msg)
                    if rid:
                        await _write_message(writer, {
                            "type": MSG_RESPONSE,
                            "rid": rid,
                            "ok": True,
                            "data": result,
                        })
                except Exception as e:
                    log.error("IPC: request handler error for %s: %s", msg.get("type"), e)
                    if rid:
                        await _write_message(writer, {
                            "type": MSG_RESPONSE,
                            "rid": rid,
                            "ok": False,
                            "error": str(e),
                        })

        except (ConnectionError, asyncio.IncompleteReadError, OSError):
            pass
        except Exception as e:
            log.error("IPC: unexpected error in connection handler: %s", e)
        finally:
            if channel_id:
                if is_primary:
                    self._workers.pop(channel_id, None)
                    log.info("IPC: worker disconnected for channel %s", channel_id)
                else:
                    aux_list = self._aux_connections.get(channel_id, [])
                    if writer in aux_list:
                        aux_list.remove(writer)
                    log.info("IPC: auxiliary connection disconnected for channel %s", channel_id)
            writer.close()


# ---------------------------------------------------------------------------
# IPCClient — used by workers
# ---------------------------------------------------------------------------

class IPCClient:
    """Unix domain socket client that connects to the manager's IPC server.

    Provides an async request/response API that mirrors AiChatAPI's interface.
    Also receives pushed events from the manager.
    """

    def __init__(self, socket_path: str, channel_id: str):
        self.socket_path = socket_path
        self.channel_id = channel_id
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._pending: dict[str, asyncio.Future] = {}  # rid -> Future
        self._event_handlers: list[Callable[[dict], Awaitable[None]]] = []
        self._listen_task: asyncio.Task | None = None

    def on_event(self, handler: Callable[[dict], Awaitable[None]]) -> None:
        """Register a handler for pushed events from the manager."""
        self._event_handlers.append(handler)

    async def connect(self) -> None:
        """Connect to the manager's IPC server and register."""
        self._reader, self._writer = await asyncio.open_unix_connection(self.socket_path)
        await _write_message(self._writer, {
            "type": MSG_REGISTER,
            "channel_id": self.channel_id,
        })
        self._listen_task = asyncio.create_task(self._listen())
        log.info("IPC: connected to manager, registered channel %s", self.channel_id)

    async def close(self) -> None:
        """Close the connection."""
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self._writer:
            self._writer.close()

    async def _listen(self) -> None:
        """Background task: read messages from manager, dispatch responses and events."""
        try:
            async for msg in _read_messages(self._reader):
                msg_type = msg.get("type")

                if msg_type == MSG_RESPONSE:
                    rid = msg.get("rid")
                    future = self._pending.pop(rid, None) if rid else None
                    if future and not future.done():
                        future.set_result(msg)
                    continue

                # Pushed event from manager
                for handler in self._event_handlers:
                    try:
                        await handler(msg)
                    except Exception as e:
                        log.error("IPC: event handler error: %s", e)

        except (ConnectionError, asyncio.IncompleteReadError, OSError):
            log.warning("IPC: connection to manager lost")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("IPC: unexpected error in listener: %s", e)
        finally:
            # Fail all pending requests
            for rid, future in self._pending.items():
                if not future.done():
                    future.set_exception(ConnectionError("IPC connection lost"))
            self._pending.clear()

    async def _request(self, msg: dict, timeout: float = 30.0) -> dict:
        """Send a request and await the response."""
        rid = _generate_rid()
        msg["rid"] = rid

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[rid] = future

        try:
            await _write_message(self._writer, msg)
            result = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(rid, None)
            raise TimeoutError(f"IPC request timed out after {timeout}s: {msg.get('type')}")

        if not result.get("ok"):
            raise RuntimeError(f"IPC request failed: {result.get('error', 'unknown')}")
        return result.get("data", {})

    async def _fire_and_forget(self, msg: dict) -> None:
        """Send a message without waiting for a response."""
        await _write_message(self._writer, msg)

    # -----------------------------------------------------------------------
    # AiChatAPI-compatible interface
    # -----------------------------------------------------------------------

    async def send_message(self, content: str) -> dict:
        return await self._request({
            "type": MSG_SEND_MESSAGE,
            "channel_id": self.channel_id,
            "content": content,
        })

    async def mark_read(self, message_ids: list[str]) -> dict:
        if not message_ids:
            return {"marked": []}
        return await self._request({
            "type": MSG_MARK_READ,
            "channel_id": self.channel_id,
            "message_ids": message_ids,
        })

    async def send_tool_status(
        self, status: str, tool: str = "", description: str = ""
    ) -> None:
        """Fire-and-forget tool status update."""
        msg: dict[str, Any] = {
            "type": MSG_TOOL_STATUS,
            "channel_id": self.channel_id,
            "status": status,
        }
        if tool:
            msg["tool"] = tool
        if description:
            msg["description"] = description
        await self._fire_and_forget(msg)

    async def get_session(self):
        """Not needed over IPC — returns None."""
        return None

    async def get_unread_messages(self) -> list[dict]:
        result = await self._request({
            "type": MSG_GET_UNREAD,
            "channel_id": self.channel_id,
        })
        return result.get("messages", [])

    async def create_interaction(
        self,
        interaction_type: str,
        content: str,
        *,
        options: list[dict] | None = None,
        multi_select: bool = False,
    ) -> dict:
        msg: dict[str, Any] = {
            "type": MSG_CREATE_INTERACTION,
            "channel_id": self.channel_id,
            "interaction_type": interaction_type,
            "content": content,
        }
        if options:
            msg["options"] = options
            msg["multi_select"] = multi_select
        return await self._request(msg)

    async def send_event(self, event_type: str) -> dict:
        return await self._request({
            "type": MSG_SEND_EVENT,
            "channel_id": self.channel_id,
            "event_type": event_type,
        })

    async def report_directories(
        self,
        working_directory: str,
        additional_directories: list[str] | None = None,
    ) -> dict:
        msg: dict[str, Any] = {
            "type": MSG_REPORT_DIRECTORIES,
            "channel_id": self.channel_id,
            "working_directory": working_directory,
        }
        if additional_directories:
            msg["additional_directories"] = additional_directories
        return await self._request(msg)

    async def download_attachment(self, url: str) -> str:
        """Download an attachment via the manager and get back a temp file path."""
        result = await self._request({
            "type": MSG_DOWNLOAD_ATTACHMENT,
            "channel_id": self.channel_id,
            "url": url,
        }, timeout=60.0)
        return result["path"]


# ---------------------------------------------------------------------------
# Wire helpers — NDJSON framing over asyncio streams
# ---------------------------------------------------------------------------

async def _write_message(writer: asyncio.StreamWriter, msg: dict) -> None:
    """Write a JSON message terminated by newline."""
    data = json.dumps(msg, separators=(",", ":")).encode() + b"\n"
    writer.write(data)
    await writer.drain()


async def _read_messages(reader: asyncio.StreamReader):
    """Async generator that yields parsed JSON messages from a stream."""
    while True:
        line = await reader.readline()
        if not line:
            break  # EOF
        try:
            yield json.loads(line)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning("IPC: failed to parse message: %s", e)
