#!/usr/bin/env python3
"""AI.CHAT Codex CLI agent — Codex app-server protocol client.

Mirrors aichat_agent.py's role but drives OpenAI Codex CLI instead of Claude Code.
Communicates with the Codex app-server over stdio JSON-RPC and integrates with
the AI.CHAT manager via the same IPC layer.

Usage:
    uv run python3 codex_agent.py [initial prompt]
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

from aichat_api import AiChatAPI
from aichat_interactions import InteractionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [Codex] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Codex App-Server JSON-RPC client
# ---------------------------------------------------------------------------

class CodexAppServer:
    """Low-level JSON-RPC client over stdin/stdout of `codex app-server`."""

    def __init__(self, config_overrides: list[str] | None = None):
        self._proc: asyncio.subprocess.Process | None = None
        self._next_id = 1
        self._pending: dict[int | str, asyncio.Future] = {}
        self._notification_handlers: dict[str, list] = {}
        self._request_handlers: dict[str, Any] = {}
        self._reader_task: asyncio.Task | None = None
        self._config_overrides = config_overrides or []

    def on_notification(self, method: str, handler):
        """Register a handler for server notifications."""
        self._notification_handlers.setdefault(method, []).append(handler)

    def on_request(self, method: str, handler):
        """Register a handler for server-initiated requests (approvals etc)."""
        self._request_handlers[method] = handler

    async def start(self) -> dict:
        """Spawn codex app-server, perform initialize handshake, return server capabilities."""
        cmd = ["codex", "app-server"]
        for override in self._config_overrides:
            cmd.extend(["-c", override])

        self._proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        log.info("Codex app-server started (pid=%s)", self._proc.pid)

        # Start background reader
        self._reader_task = asyncio.create_task(self._read_loop())

        # Initialize handshake
        result = await self.send_request("initialize", {
            "clientInfo": {"name": "aichat", "version": "1.0"},
            "capabilities": {},
        })
        # Send initialized notification
        await self.send_notification("initialized")
        log.info("Codex app-server initialized")
        return result

    async def send_request(self, method: str, params: dict, timeout: float = 120.0) -> dict:
        """Send a JSON-RPC request and await the response."""
        req_id = self._next_id
        self._next_id += 1

        msg = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[req_id] = future

        await self._write(msg)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise TimeoutError(f"Codex request {method} timed out after {timeout}s")

        if "error" in result:
            raise RuntimeError(f"Codex error in {method}: {result['error']}")
        return result.get("result", {})

    async def send_notification(self, method: str, params: dict | None = None) -> None:
        """Send a JSON-RPC notification (no id, no response expected)."""
        msg: dict = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        await self._write(msg)

    async def send_response(self, req_id: int | str, result: dict) -> None:
        """Send a JSON-RPC response to a server-initiated request."""
        msg = {"jsonrpc": "2.0", "id": req_id, "result": result}
        await self._write(msg)

    async def _write(self, msg: dict) -> None:
        """Write a JSON-RPC message to stdin."""
        if not self._proc or not self._proc.stdin:
            raise ConnectionError("Codex app-server not running")
        data = json.dumps(msg, separators=(",", ":")).encode() + b"\n"
        self._proc.stdin.write(data)
        await self._proc.stdin.drain()

    async def _read_loop(self) -> None:
        """Background task: read stdout, dispatch responses/notifications/requests."""
        assert self._proc and self._proc.stdout
        try:
            while True:
                line = await self._proc.stdout.readline()
                if not line:
                    log.warning("Codex app-server stdout closed")
                    break
                try:
                    msg = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue

                if "id" in msg and "method" in msg:
                    # Server-initiated request
                    await self._handle_server_request(msg)
                elif "id" in msg:
                    # Response to our request
                    req_id = msg["id"]
                    future = self._pending.pop(req_id, None)
                    if future and not future.done():
                        future.set_result(msg)
                elif "method" in msg:
                    # Server notification
                    await self._handle_notification(msg)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Codex reader error: %s", e)
        finally:
            # Fail pending requests
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(ConnectionError("Codex app-server disconnected"))
            self._pending.clear()

    async def _handle_notification(self, msg: dict) -> None:
        """Dispatch a server notification to registered handlers."""
        method = msg.get("method", "")
        params = msg.get("params", {})
        handlers = self._notification_handlers.get(method, [])
        for handler in handlers:
            try:
                await handler(params)
            except Exception as e:
                log.error("Notification handler error for %s: %s", method, e)

    async def _handle_server_request(self, msg: dict) -> None:
        """Dispatch a server-initiated request to the registered handler."""
        method = msg.get("method", "")
        params = msg.get("params", {})
        req_id = msg["id"]

        handler = self._request_handlers.get(method)
        if handler:
            try:
                result = await handler(req_id, params)
                # Handler may send response itself; if it returns a dict, send it
                if isinstance(result, dict):
                    await self.send_response(req_id, result)
            except Exception as e:
                log.error("Request handler error for %s: %s", method, e)
                await self.send_response(req_id, {"error": str(e)})
        else:
            log.warning("No handler for server request: %s", method)
            # Auto-accept unknown requests
            await self.send_response(req_id, {"decision": "accept"})

    async def stop(self) -> None:
        """Kill the app-server subprocess."""
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self._proc:
            try:
                self._proc.terminate()
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except (ProcessLookupError, asyncio.TimeoutError):
                try:
                    self._proc.kill()
                except ProcessLookupError:
                    pass


# ---------------------------------------------------------------------------
# SSE context (same as Claude agent)
# ---------------------------------------------------------------------------

SSE_CONTEXT = (
    "IMPORTANT: You are communicating with the user through a chat interface.\n"
    "- The user CANNOT see your text responses. The ONLY way to communicate "
    "with the user is by calling the mcp__aichat__send tool. "
    "Every time you want to tell the user something, you MUST call "
    "mcp__aichat__send. Never just write a text response "
    "and assume the user will see it.\n"
    "- This is a casual chat. Be concise and human. Don't narrate your "
    "thought process or explain what you're about to do — just do it. "
    "Keep messages short and natural.\n"
)

COMPACT_PROMPT = (
    "Context is about to be reset. Before that happens, provide a detailed "
    "summary for your successor agent covering:\n"
    "- What we've worked on this session (tasks completed, in-progress, and abandoned)\n"
    "- Any directives or preferences the user expressed that should carry forward\n"
    "- Current task status and next steps\n"
    "- Relevant project architecture and patterns discovered\n"
    "- Key file paths and code locations referenced\n"
    "- Any decisions made and their rationale\n\n"
    "Write this as a context briefing — your replacement will receive it as their "
    "starting context. Be thorough. Do NOT use the aichat_send tool — just write "
    "your summary as a direct text response."
)


def _parse_slash_command(content: str) -> str | None:
    stripped = content.strip().lower()
    if stripped in ("/clear", "/compact"):
        return stripped
    return None


# ---------------------------------------------------------------------------
# MCP config generation
# ---------------------------------------------------------------------------

def _build_mcp_config_overrides(ipc_socket: str, channel_id: str) -> list[str]:
    """Build -c flag overrides for codex app-server to configure our MCP server.

    Uses -c flags so Codex keeps its normal CODEX_HOME (and auth tokens).
    """
    mcp_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "aichat_mcp_server.py")

    return [
        'mcp_servers.aichat.type="stdio"',
        'mcp_servers.aichat.command="uv"',
        f'mcp_servers.aichat.args=["run", "python3", "{mcp_script}"]',
        f'mcp_servers.aichat.env.AICHAT_IPC_SOCKET="{ipc_socket}"',
        f'mcp_servers.aichat.env.AICHAT_CHANNEL_ID="{channel_id}"',
    ]


# ---------------------------------------------------------------------------
# Event listener (IPC or SSE)
# ---------------------------------------------------------------------------

def _parse_sse_event(line: str, channel_id: str | None) -> dict | None:
    """Parse an SSE line, returning event data if relevant to our channel."""
    if not line.startswith("data:"):
        return None
    try:
        event = json.loads(line[5:].strip())
    except (json.JSONDecodeError, ValueError):
        return None

    if channel_id and event.get("channel_id") and event.get("channel_id") != channel_id:
        return None

    event_type = event.get("type")

    if event_type == "aichat:message" and event.get("sender") == "user":
        return {
            "_event_type": "message",
            "message_id": event.get("message_id"),
            "content": event.get("content", ""),
            "attachments": event.get("attachments", []),
        }

    if (
        event_type == "aichat:message"
        and event.get("sender") == "event"
        and event.get("content", "").startswith("plan:")
    ):
        return {"_event_type": "plan", "content": event.get("content", "")}

    if event_type == "aichat:interaction-response":
        return {
            "_event_type": "interaction-response",
            "interaction_id": event.get("interaction_id"),
            "action": event.get("action"),
            "answer": event.get("answer", ""),
            "reason": event.get("reason", ""),
        }

    return None


async def listen_sse(
    message_queue: asyncio.Queue,
    interactions: InteractionManager,
    api: AiChatAPI,
) -> None:
    """Listen to Skrift SSE stream (standalone mode only)."""
    import httpx

    while True:
        try:
            cookies = await api.get_session()
            log.info("SSE session acquired, connecting to notification stream")
            async with httpx.AsyncClient(cookies=cookies, timeout=httpx.Timeout(None, connect=10)) as http:
                async with http.stream("GET", f"{api.base_url}/notifications/stream") as response:
                    async for line in response.aiter_lines():
                        event_data = _parse_sse_event(line, api.channel_id)
                        if event_data is None:
                            continue
                        event_type = event_data.pop("_event_type")
                        if event_type == "message":
                            log.info("SSE: received message from Zech")
                            interactions.cancel_all()
                            await message_queue.put(event_data)
                        elif event_type == "plan":
                            await message_queue.put(event_data)
                        elif event_type == "interaction-response":
                            iid = event_data.get("interaction_id")
                            if iid:
                                interactions.deliver_response(iid, event_data)
        except Exception as e:
            log.warning("SSE error: %s, reconnecting in 5s", e)
            await asyncio.sleep(5)


async def listen_ipc(
    message_queue: asyncio.Queue,
    interactions: InteractionManager,
    ipc_client,
) -> None:
    """Listen for pushed events from manager via IPC."""
    from aichat_ipc import MSG_EVENT_MESSAGE, MSG_EVENT_PLAN, MSG_EVENT_INTERACTION_RESPONSE

    async def handle_event(msg: dict) -> None:
        msg_type = msg.get("type")
        if msg_type == MSG_EVENT_MESSAGE:
            log.info("IPC: received message from Zech")
            interactions.cancel_all()
            await message_queue.put({
                "message_id": msg.get("message_id"),
                "content": msg.get("content", ""),
                "attachments": msg.get("attachments", []),
            })
        elif msg_type == MSG_EVENT_PLAN:
            await message_queue.put({"content": msg.get("content", "")})
        elif msg_type == MSG_EVENT_INTERACTION_RESPONSE:
            iid = msg.get("interaction_id")
            if iid:
                interactions.deliver_response(iid, {
                    "interaction_id": iid,
                    "action": msg.get("action"),
                    "answer": msg.get("answer", ""),
                    "reason": msg.get("reason", ""),
                })

    ipc_client.on_event(handle_event)
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Silence reminder
# ---------------------------------------------------------------------------

async def silence_reminder_task(
    message_queue: asyncio.Queue,
    hook_state: dict,
) -> None:
    _SILENCE_THRESHOLD = 120
    _CHECK_INTERVAL = 30
    while True:
        await asyncio.sleep(_CHECK_INTERVAL)
        now = time.time()
        last_send = hook_state.get("last_send_time", now)
        last_remind = hook_state.get("last_silence_remind_bg", 0)
        if now - last_send >= _SILENCE_THRESHOLD and now - last_remind >= _SILENCE_THRESHOLD:
            hook_state["last_silence_remind_bg"] = now
            await message_queue.put({
                "_system": True,
                "content": (
                    "[System: You haven't messaged Zech in over 2 minutes. "
                    "Consider using mcp__aichat__send to update them on your progress.]"
                ),
            })


def _drain_queue(message_queue: asyncio.Queue) -> list[dict]:
    items = []
    while not message_queue.empty():
        try:
            items.append(message_queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    return items


# ---------------------------------------------------------------------------
# Main agent loop
# ---------------------------------------------------------------------------

async def run_agent(
    initial_prompt: str | None = None,
    token: str | None = None,
    device_key: str | None = None,
    device_id: str | None = None,
    channel_id: str | None = None,
    base_url: str | None = None,
    ipc_socket: str | None = None,
) -> None:
    """Main agent loop with Codex app-server backend."""

    # Choose API backend
    if ipc_socket:
        from aichat_ipc import IPCClient
        api = IPCClient(ipc_socket, channel_id)
        await api.connect()
        log.info("Using IPC for server communication via %s", ipc_socket)
    else:
        api = AiChatAPI(
            token=token,
            device_key=device_key,
            device_id=device_id,
            channel_id=channel_id,
            base_url=base_url,
        )
        log.info("Using direct HTTP for server communication")

    interactions = InteractionManager()
    message_queue: asyncio.Queue[dict] = asyncio.Queue()
    hook_state: dict = {
        "last_send_time": time.time(),
        "last_silence_remind_bg": 0,
    }

    cwd = os.getcwd()
    try:
        await api.report_directories(cwd)
    except Exception as e:
        log.warning("Failed to report working directory: %s", e)

    await api.send_tool_status("active", tool="startup", description="Agent online. (Codex)")
    hook_state["last_send_time"] = time.time()
    log.info("Codex agent started, channel=%s, cwd=%s", channel_id or "?", cwd)

    # Start event listener
    if ipc_socket:
        event_task = asyncio.create_task(listen_ipc(message_queue, interactions, api))
    else:
        event_task = asyncio.create_task(listen_sse(message_queue, interactions, api))

    reminder_task = asyncio.create_task(silence_reminder_task(message_queue, hook_state))

    next_prompt: str | None = initial_prompt

    # Build MCP config overrides for codex app-server
    config_overrides: list[str] = []
    if channel_id:
        config_overrides = _build_mcp_config_overrides(ipc_socket or "", channel_id)

    try:
        # Outer loop: each iteration is a fresh Codex thread
        while True:
            codex = CodexAppServer(config_overrides=config_overrides)

            # Register notification handlers
            agent_text_buffer: list[str] = []
            current_thread_id: str | None = None
            current_turn_id: str | None = None

            async def on_thread_started(params):
                nonlocal current_thread_id
                thread = params.get("thread", {})
                current_thread_id = thread.get("id")
                log.info("Thread started: %s", current_thread_id)

            async def on_turn_started(params):
                nonlocal current_turn_id
                turn = params.get("turn", {})
                current_turn_id = turn.get("id")
                log.info("Turn started: %s", current_turn_id)

            async def on_turn_completed(params):
                nonlocal current_turn_id
                current_turn_id = None
                await api.send_tool_status("idle")
                log.info("Turn completed")

            async def on_agent_message_delta(params):
                delta = params.get("delta", "")
                if delta:
                    agent_text_buffer.append(delta)

            async def on_item_started(params):
                item = params.get("item", {})
                item_type = item.get("type", "")
                if item_type == "commandExecution":
                    cmd = item.get("command", "")
                    await api.send_tool_status("active", tool="Bash", description=f"Running: {cmd}")
                elif item_type == "fileChange":
                    changes = item.get("changes", [])
                    if changes:
                        paths = [c.get("path", "") for c in changes if c.get("path")]
                        desc = f"Editing {', '.join(os.path.basename(p) for p in paths[:3])}" if paths else "Editing files"
                    else:
                        desc = "Editing files"
                    await api.send_tool_status("active", tool="Edit", description=desc)
                elif item_type == "mcpToolCall":
                    tool_name = item.get("tool", "")
                    server = item.get("server", "")
                    await api.send_tool_status("active", tool=f"mcp:{server}:{tool_name}", description=f"Calling {tool_name}")
                elif item_type == "agentMessage":
                    agent_text_buffer.clear()
                    await api.send_tool_status("active", tool="reasoning", description="Thinking...")
                elif item_type == "reasoning":
                    await api.send_tool_status("active", tool="reasoning", description="Reasoning...")

            async def on_item_completed(params):
                item = params.get("item", {})
                item_type = item.get("type", "")
                if item_type == "commandExecution":
                    output = item.get("aggregatedOutput", "")
                    cmd = item.get("command", "")
                    if output:
                        await api.send_tool_status("active", tool="Bash", description=f"output:Running: {cmd}\n{output}")
                    await api.send_tool_status("done", tool="Bash")
                elif item_type == "fileChange":
                    await api.send_tool_status("done", tool="Edit")
                elif item_type == "mcpToolCall":
                    tool_name = item.get("tool", "")
                    server = item.get("server", "")
                    # Check if this was an aichat send call — update send time
                    if server == "aichat" and tool_name == "send":
                        hook_state["last_send_time"] = time.time()
                    await api.send_tool_status("done", tool=f"mcp:{server}:{tool_name}")
                elif item_type == "agentMessage":
                    text = item.get("text", "")
                    if text:
                        await api.send_tool_status("active", tool="reasoning", description=text)

            async def on_command_output_delta(params):
                # Just let it stream; we report final output on item/completed
                pass

            async def on_file_change_delta(params):
                pass

            async def on_error(params):
                error = params.get("error", {})
                msg = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                log.error("Codex error: %s (willRetry=%s)", msg, params.get("willRetry"))

            # Approval handlers (auto-accept all)
            async def on_command_approval(req_id, params):
                cmd = params.get("command", "")
                log.info("Auto-approving command: %s", cmd)
                return {"decision": "accept"}

            async def on_file_change_approval(req_id, params):
                log.info("Auto-approving file change")
                return {"decision": "accept"}

            async def on_apply_patch_approval(req_id, params):
                log.info("Auto-approving patch")
                return {"decision": "accept"}

            async def on_exec_command_approval(req_id, params):
                cmd = params.get("command", "")
                log.info("Auto-approving exec: %s", cmd)
                return {"decision": "accept"}

            # Register handlers
            codex.on_notification("thread/started", on_thread_started)
            codex.on_notification("turn/started", on_turn_started)
            codex.on_notification("turn/completed", on_turn_completed)
            codex.on_notification("item/agentMessage/delta", on_agent_message_delta)
            codex.on_notification("item/started", on_item_started)
            codex.on_notification("item/completed", on_item_completed)
            codex.on_notification("item/commandExecution/outputDelta", on_command_output_delta)
            codex.on_notification("item/fileChange/outputDelta", on_file_change_delta)
            codex.on_notification("error", on_error)

            codex.on_request("item/commandExecution/requestApproval", on_command_approval)
            codex.on_request("item/fileChange/requestApproval", on_file_change_approval)
            codex.on_request("applyPatchApproval", on_apply_patch_approval)
            codex.on_request("execCommandApproval", on_exec_command_approval)

            reset_reason: str | None = None

            try:
                await codex.start()

                # Start a thread
                instructions = SSE_CONTEXT
                if next_prompt:
                    instructions += "\n\n" + next_prompt
                next_prompt = None

                thread_params: dict[str, Any] = {
                    "approvalPolicy": "never",  # We handle approvals ourselves
                    "cwd": cwd,
                    "sandbox": "danger-full-access",
                }
                thread_result = await codex.send_request("thread/start", thread_params)
                thread_info = thread_result.get("thread", thread_result)
                current_thread_id = thread_info.get("id") if isinstance(thread_info, dict) else None

                if not current_thread_id:
                    log.error("Failed to get thread ID from thread/start result: %s", thread_result)
                    raise RuntimeError("No thread ID returned")

                log.info("Codex thread started: %s", current_thread_id)

                # Send initial turn with instructions
                initial_input = instructions if instructions.strip() else "Check in with Zech."
                turn_result = await codex.send_request("turn/start", {
                    "threadId": current_thread_id,
                    "input": [{"type": "text", "text": initial_input}],
                })
                log.info("Initial turn started")

                # Wait for turn to complete (notifications handle the rest)
                # Then enter message loop
                while True:
                    pending = _drain_queue(message_queue)
                    if not pending:
                        msg_data = await message_queue.get()
                    elif len(pending) == 1:
                        msg_data = pending[0]
                    else:
                        real_msgs = [m for m in pending if not m.get("_system")]
                        system_msgs = [m for m in pending if m.get("_system")]
                        if real_msgs:
                            msg_data = real_msgs[-1]
                        elif system_msgs:
                            msg_data = system_msgs[-1]
                        else:
                            continue

                    # System messages
                    if msg_data.get("_system"):
                        content = msg_data.get("content", "")
                        if content and current_thread_id:
                            # If a turn is active, use steer; otherwise start a new turn
                            if current_turn_id:
                                try:
                                    await codex.send_request("turn/steer", {
                                        "threadId": current_thread_id,
                                        "expectedTurnId": current_turn_id,
                                        "input": [{"type": "text", "text": content}],
                                    })
                                except Exception:
                                    # Turn may have ended; start new one
                                    await codex.send_request("turn/start", {
                                        "threadId": current_thread_id,
                                        "input": [{"type": "text", "text": content}],
                                    })
                            else:
                                await codex.send_request("turn/start", {
                                    "threadId": current_thread_id,
                                    "input": [{"type": "text", "text": content}],
                                })
                        continue

                    # Plan events: buffer
                    if msg_data.get("content", "").startswith("plan:"):
                        continue

                    content = msg_data.get("content", "")
                    message_id = msg_data.get("message_id")

                    if message_id:
                        try:
                            await api.mark_read([message_id])
                        except Exception:
                            pass

                    command = _parse_slash_command(content)

                    if command == "/clear":
                        log.info("/clear — resetting thread")
                        await api.send_message("Context cleared.")
                        hook_state["last_send_time"] = time.time()
                        next_prompt = None
                        reset_reason = "clear"
                        break

                    if command == "/compact":
                        log.info("/compact — summarizing then resetting")
                        await api.send_tool_status("active", tool="compact", description="Compacting context...")

                        if current_thread_id:
                            await codex.send_request("turn/start", {
                                "threadId": current_thread_id,
                                "input": [{"type": "text", "text": COMPACT_PROMPT}],
                            })
                            # Wait for turn completion and capture agent message
                            summary_event = asyncio.Event()
                            summary_parts: list[str] = []

                            original_on_item_completed = on_item_completed
                            original_on_turn_completed = on_turn_completed

                            async def capture_item_completed(params):
                                item = params.get("item", {})
                                if item.get("type") == "agentMessage":
                                    text = item.get("text", "")
                                    if text:
                                        summary_parts.append(text)
                                await original_on_item_completed(params)

                            async def capture_turn_completed(params):
                                await original_on_turn_completed(params)
                                summary_event.set()

                            # Temporarily swap handlers
                            codex._notification_handlers["item/completed"] = [capture_item_completed]
                            codex._notification_handlers["turn/completed"] = [capture_turn_completed]

                            try:
                                await asyncio.wait_for(summary_event.wait(), timeout=120)
                            except asyncio.TimeoutError:
                                log.warning("Compact summary timed out")

                            # Restore handlers
                            codex._notification_handlers["item/completed"] = [on_item_completed]
                            codex._notification_handlers["turn/completed"] = [on_turn_completed]

                            summary = "\n".join(summary_parts)
                            if summary.strip():
                                next_prompt = (
                                    "You are resuming work after a context compaction. "
                                    "Here is a summary from the previous context:\n\n"
                                    f"{summary}\n\n"
                                    "Continue from where things left off. Check in with Zech."
                                )
                            else:
                                next_prompt = None

                        await api.send_message("Context compacted.")
                        hook_state["last_send_time"] = time.time()
                        reset_reason = "compact"
                        break

                    # Normal message
                    prompt = f"New message from Zech:\n{content}"

                    if current_thread_id:
                        # If turn is active, interrupt then start new turn
                        if current_turn_id:
                            try:
                                await codex.send_request("turn/interrupt", {
                                    "threadId": current_thread_id,
                                    "turnId": current_turn_id,
                                })
                            except Exception:
                                pass

                        await codex.send_request("turn/start", {
                            "threadId": current_thread_id,
                            "input": [{"type": "text", "text": prompt}],
                        })

            finally:
                await codex.stop()

            log.info("Codex thread reset (%s), starting fresh", reset_reason)

    except Exception as e:
        log.error("Codex agent error: %s", e)
        try:
            await api.send_message(f"Agent error: {e}")
        except Exception:
            pass
        raise
    finally:
        reminder_task.cancel()
        event_task.cancel()
        for task in (reminder_task, event_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
        if ipc_socket and hasattr(api, "close"):
            await api.close()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="AI.CHAT Codex agent")
    parser.add_argument("prompt", nargs="*", help="Initial prompt")
    parser.add_argument("--token", help="Compound auth token (legacy)")
    parser.add_argument("--device-key", help="Device private key (b64)")
    parser.add_argument("--device-id", help="Device ID")
    parser.add_argument("--channel-id", help="Channel ID")
    parser.add_argument("--base-url", help="API base URL")
    parser.add_argument("--working-directory", help="Working directory for this agent")
    parser.add_argument("--ipc-socket", help="Path to manager's IPC Unix domain socket")
    args = parser.parse_args()

    if args.working_directory and os.path.isdir(args.working_directory):
        os.chdir(args.working_directory)
        log.info("Changed working directory to: %s", args.working_directory)

    initial_prompt = " ".join(args.prompt) if args.prompt else None
    asyncio.run(run_agent(
        initial_prompt,
        token=args.token,
        device_key=args.device_key,
        device_id=args.device_id,
        channel_id=args.channel_id,
        base_url=args.base_url,
        ipc_socket=args.ipc_socket,
    ))


if __name__ == "__main__":
    main()
