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
    create_sdk_mcp_server,
    tool,
)

from aichat_api import AiChatAPI
from aichat_hooks import (
    make_interaction_hook,
    make_pre_tool_hook,
    make_post_tool_hook,
    make_stop_hook,
)
from aichat_interactions import InteractionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def make_aichat_tools(api: AiChatAPI):
    """Create SDK MCP tools for AI.CHAT messaging."""

    @tool("aichat_send", "Send a message to Zech via AI.CHAT", {"message": str})
    async def aichat_send(args):
        try:
            result = await api.send_message(args["message"])
            return {"content": [{"type": "text", "text": f"Message sent: {result.get('id', 'ok')}"}]}
        except Exception as e:
            return {"content": [{"type": "text", "text": f"Failed to send: {e}"}], "is_error": True}

    return [aichat_send]


def build_agent_options(
    api: AiChatAPI,
    interactions: InteractionManager,
) -> ClaudeAgentOptions:
    """Build ClaudeAgentOptions with AI.CHAT hooks and tools."""
    tools = make_aichat_tools(api)
    mcp_server = create_sdk_mcp_server("aichat", tools=tools)

    return ClaudeAgentOptions(
        setting_sources=["project"],
        permission_mode="bypassPermissions",
        mcp_servers={"aichat": mcp_server},
        hooks={
            "PreToolUse": [
                # Interaction hook first — intercepts AskUserQuestion/EnterPlanMode
                HookMatcher(hooks=[make_interaction_hook(api, interactions)]),
                # Regular tool status reporting
                HookMatcher(hooks=[make_pre_tool_hook(api)]),
            ],
            "PostToolUse": [HookMatcher(hooks=[make_post_tool_hook(api)])],
            "Stop": [HookMatcher(hooks=[make_stop_hook(api)])],
        },
    )


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
            "attachments": event.get("attachments", []),
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
    """Listen to Skrift SSE stream for real-time incoming messages and interaction responses."""
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


async def run_agent(
    initial_prompt: str | None = None,
    token: str | None = None,
    device_key: str | None = None,
    device_id: str | None = None,
    channel_id: str | None = None,
    base_url: str | None = None,
) -> None:
    """Main agent loop."""
    api = AiChatAPI(
        token=token,
        device_key=device_key,
        device_id=device_id,
        channel_id=channel_id,
        base_url=base_url,
    )
    interactions = InteractionManager()
    options = build_agent_options(api, interactions)
    message_queue: asyncio.Queue[dict] = asyncio.Queue()

    await api.send_message("Agent online.")
    log.info("Agent started, channel=%s", api.channel_id)

    # Start SSE listener
    sse_task = asyncio.create_task(listen_sse(message_queue, interactions, api))

    try:
        async with ClaudeSDKClient(options=options) as client:
            # Determine initial prompt
            sse_context = (
                "IMPORTANT: You are running inside the Agent SDK wrapper. "
                "Messages from Zech are delivered to you automatically via SSE — "
                "do NOT set up cron jobs, /loop, or polling for messages. "
                "Do NOT run aichat-unread or aichat-read CLI commands. "
                "Use the aichat_send tool to send messages to Zech.\n\n"
            )
            if initial_prompt:
                prompt = sse_context + initial_prompt
            else:
                prompt = sse_context + "Check in with Zech."

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
                message_id = msg_data.get("message_id")
                attachments = msg_data.get("attachments", [])

                # Mark as read now that we're delivering it to Claude
                if message_id:
                    try:
                        await api.mark_read([message_id])
                    except Exception as e:
                        log.warning("Failed to mark message read: %s", e)

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
    import argparse
    parser = argparse.ArgumentParser(description="AI.CHAT Agent SDK wrapper")
    parser.add_argument("prompt", nargs="*", help="Initial prompt")
    parser.add_argument("--token", help="Compound auth token (legacy)")
    parser.add_argument("--device-key", help="Device private key (b64)")
    parser.add_argument("--device-id", help="Device ID")
    parser.add_argument("--channel-id", help="Channel ID")
    parser.add_argument("--base-url", help="API base URL")
    args = parser.parse_args()

    initial_prompt = " ".join(args.prompt) if args.prompt else None
    asyncio.run(run_agent(
        initial_prompt,
        token=args.token,
        device_key=args.device_key,
        device_id=args.device_id,
        channel_id=args.channel_id,
        base_url=args.base_url,
    ))


if __name__ == "__main__":
    main()
