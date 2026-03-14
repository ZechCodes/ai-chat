#!/usr/bin/env python3
"""AI.CHAT Agent SDK wrapper — optional substitute for hooks + CLI scripts.

Launches Claude Code as an SDK-managed agent with hooks that report tool use,
stop state, and messages directly to AI.CHAT API endpoints.

When launched with --ipc-socket, all server communication goes through the
manager process via IPC. Otherwise falls back to direct HTTP API calls.

Usage:
    uv run python3 aichat_agent.py [initial prompt]
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    TextBlock,
    create_sdk_mcp_server,
    tool,
)

from aichat_api import AiChatAPI
from aichat_hooks import (
    make_can_use_tool,
    make_interaction_hook,
    make_pre_tool_hook,
    make_post_tool_hook,
    make_stop_hook,
    make_pre_compact_hook,
)
from aichat_interactions import InteractionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [Claude] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SDK monkey-patch: prevent premature stdin closure (SDK bug in 0.1.48)
#
# The SDK closes stdin after the first assistant message arrives, but hooks
# need the control stream open for the entire query duration. Without this
# patch, ALL hook callbacks fail with "Stream closed" errors because the CLI
# can't send control_requests back to the Python process.
# ---------------------------------------------------------------------------
from claude_agent_sdk._internal import query as _sdk_query  # noqa: E402


async def _patched_wait_for_result_and_end_input(self) -> None:
    """No-op replacement — keep stdin open for hooks throughout the query."""
    if self.sdk_mcp_servers or self.hooks:
        log.debug("Keeping stdin open for hook callbacks (patched)")
    else:
        await self.transport.end_input()


_sdk_query.Query.wait_for_result_and_end_input = _patched_wait_for_result_and_end_input


def make_aichat_tools(api, message_queue, skipped_messages, hook_state):
    """Create SDK MCP tools for AI.CHAT messaging and directory management."""

    @tool("send", "Send a message to the user via AI.CHAT", {"message": str})
    async def aichat_send(args):
        try:
            result = await api.send_message(args["message"])
            hook_state["last_send_time"] = time.time()
            return {"content": [{"type": "text", "text": f"Message sent: {result.get('id', 'ok')}"}]}
        except Exception as e:
            return {"content": [{"type": "text", "text": f"Failed to send: {e}"}], "is_error": True}

    @tool(
        "read_unread",
        "Read unread messages from Zech. Call this when notified about unread messages.",
        {},
    )
    async def aichat_read_unread(args):
        messages = []
        # Drain skipped (older) messages first
        while skipped_messages:
            messages.append(skipped_messages.pop(0))
        # Then drain the live queue (skip system messages)
        while not message_queue.empty():
            try:
                m = message_queue.get_nowait()
                if not m.get("_system"):
                    messages.append(m)
            except asyncio.QueueEmpty:
                break
        if not messages:
            return {"content": [{"type": "text", "text": "No unread messages."}]}
        # Mark all as read
        ids_to_mark = [m["message_id"] for m in messages if m.get("message_id")]
        if ids_to_mark:
            try:
                await api.mark_read(ids_to_mark)
            except Exception as e:
                log.warning("Failed to mark messages read: %s", e)
        # Format output
        lines = []
        for m in messages:
            content = m.get("content", "")
            att = m.get("attachments", [])
            att_note = f" [{len(att)} attachment(s)]" if att else ""
            lines.append(f"Zech: {content}{att_note}")
        return {"content": [{"type": "text", "text": "\n".join(lines)}]}

    @tool(
        "set_directories",
        "Report your working directory and any additional project directories you are accessing. "
        "Call this when you start working in a new directory or need access to additional project roots.",
        {"working_directory": str, "additional_directories": list},
    )
    async def aichat_set_directories(args):
        try:
            working_dir = args.get("working_directory", "")
            additional = args.get("additional_directories", [])
            await api.report_directories(working_dir, additional or None)
            return {"content": [{"type": "text", "text": "Directories updated."}]}
        except Exception as e:
            return {"content": [{"type": "text", "text": f"Failed to update directories: {e}"}], "is_error": True}

    return [aichat_send, aichat_read_unread, aichat_set_directories]


def build_agent_options(
    api,
    interactions: InteractionManager,
    message_queue: asyncio.Queue,
    skipped_messages: list,
    hook_state: dict,
) -> ClaudeAgentOptions:
    """Build ClaudeAgentOptions with AI.CHAT hooks and tools."""
    tools = make_aichat_tools(api, message_queue, skipped_messages, hook_state)
    mcp_server = create_sdk_mcp_server("aichat", tools=tools)

    return ClaudeAgentOptions(
        model="opus",
        setting_sources=["project"],
        permission_mode="plan",
        can_use_tool=make_can_use_tool(api, interactions, hook_state),
        mcp_servers={"aichat": mcp_server},
        hooks={
            "PreToolUse": [
                # Interaction hook — intercepts AskUserQuestion
                HookMatcher(hooks=[make_interaction_hook(api, interactions)]),
                # Regular tool status reporting + Write tracking
                HookMatcher(hooks=[make_pre_tool_hook(api, hook_state)]),
            ],
            "PostToolUse": [HookMatcher(hooks=[make_post_tool_hook(api, message_queue, hook_state)])],
            "PreCompact": [HookMatcher(hooks=[make_pre_compact_hook(api)])],
            "Stop": [HookMatcher(hooks=[make_stop_hook(api)])],
        },
    )


async def handle_response_message(message, api) -> str | None:
    """Report agent response messages as reasoning tool status.

    AssistantMessage text blocks are internal narration (explicit aichat-send
    calls handle real messages to the user), so they're sent as tool status
    updates rather than chat messages.

    Returns the text content if any (used by /compact to capture summaries).
    """
    if isinstance(message, AssistantMessage):
        text_parts = [
            block.text for block in message.content
            if isinstance(block, TextBlock)
        ]
        if text_parts:
            text = "\n".join(text_parts)
            await api.send_tool_status(
                status="active",
                tool="reasoning",
                description=text,
            )
            return text
    return None


# ---------------------------------------------------------------------------
# Slash commands: /clear and /compact
# ---------------------------------------------------------------------------

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
    """Return the slash command name if the message is a slash command, else None."""
    stripped = content.strip().lower()
    if stripped in ("/clear", "/compact"):
        return stripped
    return None


def _parse_sse_event(line: str, channel_id: str | None) -> dict | None:
    """Parse an SSE line, returning event data if relevant to our channel."""
    if not line.startswith("data:"):
        return None
    try:
        event = json.loads(line[5:].strip())
    except (json.JSONDecodeError, ValueError):
        return None

    # Check channel match
    if channel_id and event.get("channel_id") and event.get("channel_id") != channel_id:
        return None

    event_type = event.get("type")

    # User messages → message queue
    if (
        event_type == "aichat:message"
        and event.get("sender") == "user"
    ):
        return {
            "_event_type": "message",
            "message_id": event.get("message_id"),
            "content": event.get("content", ""),
            "attachments": event.get("attachments") or [],
        }

    # Plan mode events → message queue
    if (
        event_type == "aichat:message"
        and event.get("sender") == "event"
        and event.get("content", "").startswith("plan:")
    ):
        return {
            "_event_type": "plan",
            "content": event.get("content", ""),
        }

    # Interaction responses → interaction manager
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
    """Listen to Skrift SSE stream for real-time incoming messages and interaction responses.

    Only used when running without IPC (standalone mode).
    """
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
                            log.info("SSE: plan event: %s", event_data.get("content"))
                            await message_queue.put(event_data)

                        elif event_type == "interaction-response":
                            interaction_id = event_data.get("interaction_id")
                            if interaction_id:
                                log.info("SSE: received interaction response %s", interaction_id)
                                interactions.deliver_response(interaction_id, event_data)

        except httpx.HTTPError as e:
            log.warning("SSE connection error: %s, reconnecting in 5s", e)
            await asyncio.sleep(5)
        except Exception as e:
            log.error("SSE unexpected error: %s, reconnecting in 10s", e)
            await asyncio.sleep(10)


async def listen_ipc(
    message_queue: asyncio.Queue,
    interactions: InteractionManager,
    ipc_client,
) -> None:
    """Listen for pushed events from manager via IPC.

    Replaces listen_sse when running with --ipc-socket.
    """
    from aichat_ipc import MSG_EVENT_MESSAGE, MSG_EVENT_PLAN, MSG_EVENT_INTERACTION_RESPONSE

    async def handle_event(msg: dict) -> None:
        msg_type = msg.get("type")

        if msg_type == MSG_EVENT_MESSAGE:
            log.info("IPC: received message from Zech")
            interactions.cancel_all()
            await message_queue.put({
                "message_id": msg.get("message_id"),
                "content": msg.get("content", ""),
                "attachments": msg.get("attachments") or [],
            })

        elif msg_type == MSG_EVENT_PLAN:
            log.info("IPC: plan event: %s", msg.get("content"))
            await message_queue.put({
                "content": msg.get("content", ""),
            })

        elif msg_type == MSG_EVENT_INTERACTION_RESPONSE:
            interaction_id = msg.get("interaction_id")
            if interaction_id:
                log.info("IPC: received interaction response %s", interaction_id)
                interactions.deliver_response(interaction_id, {
                    "interaction_id": interaction_id,
                    "action": msg.get("action"),
                    "answer": msg.get("answer", ""),
                    "reason": msg.get("reason", ""),
                })

    ipc_client.on_event(handle_event)
    # The IPC client's _listen task handles the actual reading.
    # We just need to keep this coroutine alive so the task group doesn't exit.
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


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
    "- For complex tasks, use planning mode instead of detailing plans "
    "in chat messages.\n"
    "- When you have a yes/no question or a question with 2-3 clear options, "
    "use the AskUserQuestion tool to present choices instead of asking "
    "in a chat message.\n\n"
)


async def silence_reminder_task(
    message_queue: asyncio.Queue,
    hook_state: dict,
) -> None:
    """Background task that injects a silence reminder into the queue when idle."""
    _SILENCE_THRESHOLD = 120  # seconds
    _CHECK_INTERVAL = 30  # seconds

    while True:
        await asyncio.sleep(_CHECK_INTERVAL)
        now = time.time()
        last_send = hook_state.get("last_send_time", now)
        last_remind = hook_state.get("last_silence_remind_bg", 0)
        if not hook_state.get("working"):
            continue  # Don't nudge when idle
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
    """Drain all currently pending items from the queue without blocking."""
    items = []
    while not message_queue.empty():
        try:
            items.append(message_queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    return items


async def run_agent(
    initial_prompt: str | None = None,
    token: str | None = None,
    device_key: str | None = None,
    device_id: str | None = None,
    channel_id: str | None = None,
    base_url: str | None = None,
    ipc_socket: str | None = None,
) -> None:
    """Main agent loop with support for /clear and /compact context resets."""

    # Choose API backend: IPC (via manager) or direct HTTP
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
    skipped_messages: list[dict] = []  # Older messages deferred by latest-first logic
    hook_state: dict = {
        "last_send_time": time.time(),
        "last_unread_notify": 0,
        "last_silence_remind": 0,
        "last_silence_remind_bg": 0,
    }

    # Report working directory to server
    cwd = os.getcwd()
    try:
        await api.report_directories(cwd)
        log.info("Reported working directory: %s", cwd)
    except Exception as e:
        log.warning("Failed to report working directory: %s", e)

    await api.send_tool_status("active", tool="startup", description="Agent online.")
    hook_state["last_send_time"] = time.time()
    log.info("Agent started, channel=%s, cwd=%s", channel_id or getattr(api, 'channel_id', '?'), cwd)

    # Start event listener (IPC or SSE depending on mode)
    if ipc_socket:
        event_task = asyncio.create_task(listen_ipc(message_queue, interactions, api))
    else:
        event_task = asyncio.create_task(listen_sse(message_queue, interactions, api))

    # Start silence reminder background task
    reminder_task = asyncio.create_task(silence_reminder_task(message_queue, hook_state))

    # next_prompt carries context across resets (None = default check-in)
    next_prompt: str | None = initial_prompt

    try:
        # Outer loop: each iteration is a fresh agent context
        while True:
            options = build_agent_options(api, interactions, message_queue, skipped_messages, hook_state)
            reset_reason: str | None = None

            async with ClaudeSDKClient(options=options) as client:
                if next_prompt:
                    prompt = SSE_CONTEXT + next_prompt
                else:
                    prompt = SSE_CONTEXT + "Check in with Zech."
                next_prompt = None  # consumed

                # Initial query
                log.info("Sending initial prompt to agent")
                hook_state["working"] = True
                await client.query(prompt)
                async for message in client.receive_response():
                    await handle_response_message(message, api)
                hook_state["working"] = False

                # Message loop
                pending_plan_event = None
                while True:
                    # Drain queue: latest-first when multiple messages pending
                    pending = _drain_queue(message_queue)
                    if not pending:
                        # Nothing available — block for the next message
                        msg_data = await message_queue.get()
                    elif len(pending) == 1:
                        msg_data = pending[0]
                    else:
                        # Multiple pending: take the latest, store older in skipped_messages
                        # Separate system messages and real messages
                        real_msgs = [m for m in pending if not m.get("_system")]
                        system_msgs = [m for m in pending if m.get("_system")]
                        if real_msgs:
                            msg_data = real_msgs[-1]  # latest real message
                            skipped_messages.extend(real_msgs[:-1])  # older real messages
                            # System messages that queued up alongside real messages can be dropped
                        elif system_msgs:
                            msg_data = system_msgs[-1]  # latest system message
                        else:
                            continue  # shouldn't happen, but be safe

                    # --- System messages: inject content directly, no mark_read ---
                    if msg_data.get("_system"):
                        prompt = msg_data.get("content", "")
                        if prompt:
                            await client.query(prompt)
                            async for message in client.receive_response():
                                await handle_response_message(message, api)
                        continue

                    # Plan events: buffer and wait for the next user message
                    if msg_data.get("content", "").startswith("plan:"):
                        pending_plan_event = msg_data.get("content")
                        log.info("Buffered plan event: %s", pending_plan_event)
                        continue

                    log.info("Processing incoming message")
                    content = msg_data.get("content", "")
                    message_id = msg_data.get("message_id")
                    attachments = msg_data.get("attachments") or []

                    # Mark as read
                    if message_id:
                        try:
                            await api.mark_read([message_id])
                        except Exception as e:
                            log.warning("Failed to mark message read: %s", e)

                    # --- Slash command handling ---
                    command = _parse_slash_command(content)

                    if command == "/clear":
                        log.info("Slash command: /clear — resetting context")
                        await api.send_message("Context cleared.")
                        hook_state["last_send_time"] = time.time()
                        next_prompt = None
                        reset_reason = "clear"
                        break

                    if command == "/compact":
                        log.info("Slash command: /compact — summarizing then resetting")
                        await api.send_tool_status(
                            status="active",
                            tool="compact",
                            description="Compacting context...",
                        )

                        # Ask current agent to summarize the session
                        await client.query(COMPACT_PROMPT)
                        summary_parts = []
                        async for message in client.receive_response():
                            text = await handle_response_message(message, api)
                            if text:
                                summary_parts.append(text)

                        summary = "\n".join(summary_parts)
                        if summary.strip():
                            next_prompt = (
                                "You are resuming work after a context compaction. "
                                "Here is a summary from the previous context:\n\n"
                                f"{summary}\n\n"
                                "Continue from where things left off. Check in with Zech."
                            )
                            log.info("Compact summary captured (%d chars)", len(summary))
                        else:
                            log.warning("Compact produced no summary, starting fresh")
                            next_prompt = None

                        await api.send_message("Context compacted.")
                        hook_state["last_send_time"] = time.time()
                        reset_reason = "compact"
                        break

                    # --- Normal message processing ---

                    # Download image attachments to temp files for Claude to view
                    attachment_notes = []
                    for att in attachments:
                        ct = att.get("content_type", "")
                        if ct.startswith("image/"):
                            try:
                                img_path = await api.download_attachment(att["url"])
                                attachment_notes.append(
                                    f"[Image attached: {att.get('filename', 'image')} — "
                                    f"saved to {img_path}, use Read tool to view it]"
                                )
                            except Exception as e:
                                log.warning("Failed to download attachment: %s", e)
                                attachment_notes.append(
                                    f"[Image: {att.get('filename', 'image')} — "
                                    f"download failed: {e}]"
                                )

                    prompt = f"New message from Zech:\n{content}"
                    if attachment_notes:
                        prompt += "\n\n" + "\n".join(attachment_notes)

                    # Note about skipped older messages
                    if skipped_messages:
                        n = len(skipped_messages)
                        prompt += (
                            f"\n\n[System: There are {n} older message(s). "
                            f"Use the mcp__aichat__read_unread tool to read them.]"
                        )

                    # Inject plan mode instruction if a plan event was buffered
                    if pending_plan_event:
                        if pending_plan_event == "plan:enter":
                            prompt += "\n\n[System: The user has activated planning mode. Use /plan to enter plan mode before responding.]"
                        elif pending_plan_event == "plan:exit":
                            prompt += "\n\n[System: The user has deactivated planning mode.]"
                        pending_plan_event = None

                    hook_state["working"] = True
                    await client.query(prompt)
                    async for message in client.receive_response():
                        await handle_response_message(message, api)
                    hook_state["working"] = False

            # Client exited — log and loop back for fresh context
            log.info("Agent context reset (%s), starting fresh client", reset_reason)

    except Exception as e:
        log.error("Agent error: %s", e)
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
        # Clean up IPC connection if applicable
        if ipc_socket and hasattr(api, 'close'):
            await api.close()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="AI.CHAT Agent SDK wrapper")
    parser.add_argument("prompt", nargs="*", help="Initial prompt")
    parser.add_argument("--token", help="Compound auth token (legacy)")
    parser.add_argument("--device-key", help="Device private key (b64)")
    parser.add_argument("--device-id", help="Device ID")
    parser.add_argument("--channel-id", help="Channel ID")
    parser.add_argument("--base-url", help="API base URL")
    parser.add_argument("--working-directory", help="Working directory for this agent")
    parser.add_argument("--ipc-socket", help="Path to manager's IPC Unix domain socket")
    args = parser.parse_args()

    # Change to working directory if specified
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
