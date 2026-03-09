#!/usr/bin/env python3
"""AI.CHAT Agent SDK wrapper — optional substitute for hooks + CLI scripts.

Launches Claude Code as an SDK-managed agent with hooks that report tool use,
stop state, and messages directly to AI.CHAT API endpoints.

Usage:
    uv run python3 aichat_agent.py [initial prompt]
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys

import httpx

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    TextBlock,
)

from aichat_api import AiChatAPI
from aichat_hooks import make_pre_tool_hook, make_post_tool_hook, make_stop_hook

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def build_agent_options(api: AiChatAPI) -> ClaudeAgentOptions:
    """Build ClaudeAgentOptions with AI.CHAT hooks."""
    return ClaudeAgentOptions(
        setting_sources=["project"],
        permission_mode="bypassPermissions",
        hooks={
            "PreToolUse": [HookMatcher(hooks=[make_pre_tool_hook(api)])],
            "PostToolUse": [HookMatcher(hooks=[make_post_tool_hook(api)])],
            "Stop": [HookMatcher(hooks=[make_stop_hook(api)])],
        },
    )


def format_unread(messages: list[dict]) -> str | None:
    """Format unread messages into a prompt for the agent."""
    if not messages:
        return None
    parts = []
    for msg in messages:
        parts.append(f"[{msg['created_at']}] {msg['content']}")
    return "Unread messages from Zech:\n" + "\n".join(parts)


async def handle_response_message(message, api: AiChatAPI) -> None:
    """Report agent response messages as reasoning tool status.

    AssistantMessage text blocks are internal narration (explicit aichat-send
    calls handle real messages to the user), so they're sent as tool status
    updates rather than chat messages.
    """
    if isinstance(message, AssistantMessage):
        text_parts = [
            block.text for block in message.content
            if isinstance(block, TextBlock)
        ]
        if text_parts:
            await api.send_tool_status(
                status="active",
                tool="reasoning",
                description="\n".join(text_parts),
            )


def _parse_sse_event(line: str, channel_id: str | None) -> dict | None:
    """Parse an SSE line, returning message data if it's a user message for our channel."""
    if not line.startswith("data:"):
        return None
    try:
        event = json.loads(line[5:].strip())
    except (json.JSONDecodeError, ValueError):
        return None
    if (
        event.get("type") == "aichat:message"
        and event.get("sender") == "user"
        and event.get("channel_id") == channel_id
    ):
        return {
            "content": event.get("content", ""),
            "attachments": event.get("attachments", []),
        }
    return None


async def listen_sse(message_queue: asyncio.Queue, api: AiChatAPI) -> None:
    """Listen to Skrift SSE stream for real-time incoming messages."""
    while True:
        try:
            cookies = await api.get_session()
            log.info("SSE session acquired, connecting to notification stream")
            async with httpx.AsyncClient(cookies=cookies, timeout=httpx.Timeout(None, connect=10)) as http:
                async with http.stream("GET", f"{api.base_url}/notifications/stream") as response:
                    async for line in response.aiter_lines():
                        msg_data = _parse_sse_event(line, api.channel_id)
                        if msg_data is not None:
                            log.info("SSE: received message from Zech")
                            await message_queue.put(msg_data)
        except httpx.HTTPError as e:
            log.warning("SSE connection error: %s, reconnecting in 5s", e)
            await asyncio.sleep(5)
        except Exception as e:
            log.error("SSE unexpected error: %s, reconnecting in 10s", e)
            await asyncio.sleep(10)


async def run_agent(initial_prompt: str | None = None) -> None:
    """Main agent loop."""
    api = AiChatAPI()
    options = build_agent_options(api)
    message_queue: asyncio.Queue[dict] = asyncio.Queue()

    await api.send_message("Agent online.")
    log.info("Agent started, channel=%s", api.channel_id)

    # Start SSE listener
    sse_task = asyncio.create_task(listen_sse(message_queue, api))

    try:
        async with ClaudeSDKClient(options=options) as client:
            # Determine initial prompt
            sse_context = (
                "IMPORTANT: You are running inside the Agent SDK wrapper. "
                "Messages from Zech are delivered to you automatically via SSE — "
                "do NOT set up cron jobs, /loop, or polling for messages. "
                "Do NOT run aichat-unread or aichat-read to check for messages. "
                "Use aichat-send to send messages to Zech.\n\n"
            )
            if initial_prompt:
                prompt = sse_context + initial_prompt
            else:
                unread = await api.get_unread()
                prompt = sse_context + (format_unread(unread) or "Check in with Zech.")

            # Initial query
            log.info("Sending initial prompt to agent")
            await client.query(prompt)
            async for message in client.receive_response():
                await handle_response_message(message, api)

            # Main loop: wait for incoming messages
            while True:
                msg_data = await message_queue.get()
                log.info("Processing incoming message")
                content = msg_data.get("content", "")
                attachments = msg_data.get("attachments", [])

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

                await client.query(prompt)
                async for message in client.receive_response():
                    await handle_response_message(message, api)

    except Exception as e:
        log.error("Agent error: %s", e)
        try:
            await api.send_message(f"Agent error: {e}")
        except Exception:
            pass
        raise
    finally:
        sse_task.cancel()
        try:
            await sse_task
        except asyncio.CancelledError:
            pass


def main() -> None:
    initial_prompt = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(run_agent(initial_prompt))


if __name__ == "__main__":
    main()
