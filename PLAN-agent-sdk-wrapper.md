# AI.CHAT Agent SDK Wrapper — Implementation Plan

## Overview

An **optional alternative** to the current hook-based approach (relay_hook.py + aichat scripts). Built on the **Claude Agent SDK** (`claude-agent-sdk`), the wrapper launches Claude Code as an SDK-managed agent and uses hooks to report tool use, stop state, and messages directly to the AI.CHAT API endpoints. The existing hook + CLI script approach remains fully functional for standard Claude Code usage.

## Current Architecture (unchanged, still works)

```
Claude Code CLI
  ├── .claude/settings.local.json hooks → relay_hook.py → POST /api/tool-status
  ├── aichat_send.py        → POST /api/messages
  ├── aichat_unread.py      → GET  /api/messages/unread
  ├── aichat_read.py        → GET  /api/messages
  └── /loop 1m cron          → polls for messages
```

## SDK Wrapper Architecture (optional substitute)

```
aichat-agent (Python script)
  └── claude_agent_sdk.ClaudeSDKClient
        ├── PreToolUse hook   → POST /api/tool-status {status: "active", ...}
        ├── PostToolUse hook  → POST /api/tool-status {status: "done", ...}
        ├── Stop hook         → POST /api/tool-status {status: "idle"}
        ├── SSE listener      → Skrift notification stream (real-time messages)
        └── On result         → POST /api/messages (send response)
```

**Key difference**: Instead of polling (`/loop 1m` or `sleep(5)`), the wrapper connects to Skrift's SSE notification stream for instant message delivery.

## Implementation Steps

### Phase 1: Core Wrapper (`aichat_agent.py`)

```python
import asyncio
from claude_agent_sdk import ClaudeSDKClient, ClaudeAgentOptions, HookMatcher
from aichat_api import AiChatAPI

async def main():
    api = AiChatAPI()

    # Report startup
    await api.send_message("Agent online.")

    options = ClaudeAgentOptions(
        system_prompt={
            "type": "preset",
            "preset": "claude_code",
            "append": build_system_prompt(api),
        },
        setting_sources=["project"],
        permission_mode="bypassPermissions",
        hooks={
            "PreToolUse": [HookMatcher(hooks=[pre_tool_hook(api)])],
            "PostToolUse": [HookMatcher(hooks=[post_tool_hook(api)])],
            "Stop": [HookMatcher(hooks=[stop_hook(api)])],
        },
    )

    async with ClaudeSDKClient(options=options) as client:
        # Start SSE listener for real-time messages
        sse_task = asyncio.create_task(listen_sse(client, api))

        # Initial prompt from unread messages or default
        unread = await api.get_unread()
        initial_prompt = format_unread(unread) if unread else "Check in with Zech."

        await client.query(initial_prompt)
        async for message in client.receive_response():
            handle_message(message, api)

        # Continue responding to incoming messages
        while True:
            messages = await message_queue.get()
            await client.query(format_incoming(messages))
            async for message in client.receive_response():
                handle_message(message, api)
```

### Phase 2: Unified API Client (`aichat_api.py`)

Consolidate auth + HTTP into a single async client. Existing CLI scripts remain as-is.

```python
class AiChatAPI:
    """Async client for AI.CHAT API endpoints."""

    def __init__(self, base_url=None, token=None):
        # Auto-resolve token from ~/.config/aichat/tokens.json
        ...

    async def send_message(self, content: str) -> dict:
        """POST /api/messages"""

    async def get_unread(self) -> list[dict]:
        """GET /api/messages/unread"""

    async def get_messages(self, limit=10, before=None) -> list[dict]:
        """GET /api/messages"""

    async def send_tool_status(self, status, tool="", description="") -> None:
        """POST /api/tool-status"""

    def _sign_request(self, method, path) -> dict:
        """Ed25519 request signing"""
```

Uses `httpx.AsyncClient` for non-blocking HTTP.

### Phase 3: SDK Hooks

Hooks call the API directly instead of spawning shell processes.

```python
def pre_tool_hook(api: AiChatAPI):
    async def hook(input_data, tool_use_id, context):
        tool_name = input_data.get("tool_name", "")
        tool_input = input_data.get("tool_input", {})

        # Skip reporting our own API calls
        if tool_name == "Bash" and "aichat" in tool_input.get("command", ""):
            return {}

        description = describe_tool(tool_name, tool_input)
        asyncio.create_task(api.send_tool_status("active", tool_name, description))
        return {"async_": True}
    return hook

def post_tool_hook(api: AiChatAPI):
    async def hook(input_data, tool_use_id, context):
        asyncio.create_task(api.send_tool_status("done", input_data.get("tool_name", "")))
        return {"async_": True}
    return hook

def stop_hook(api: AiChatAPI):
    async def hook(input_data, tool_use_id, context):
        asyncio.create_task(api.send_tool_status("idle"))
        return {}
    return hook
```

### Phase 4: SSE Message Listener

Connect to Skrift's SSE notification stream instead of polling. The server already sends `aichat:message` events via SSE — we tap into the same stream the browser uses.

```python
async def listen_sse(client: ClaudeSDKClient, api: AiChatAPI):
    """Listen to Skrift SSE stream for real-time incoming messages."""
    async with httpx.AsyncClient() as http:
        # Exchange Ed25519 signature for a session cookie
        session_resp = await http.post(
            f"{api.base_url}/api/session",
            headers=api._sign_request("POST", "/api/session"),
        )
        session_resp.raise_for_status()
        # httpx automatically stores the Set-Cookie for subsequent requests

        # Connect to existing SSE endpoint using the session cookie
        async with http.stream("GET", f"{api.base_url}/notifications/stream") as response:
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                event = json.loads(line[5:].strip())

                # Only care about user messages on our channel
                if (event.get("type") == "aichat:message"
                        and event.get("sender") == "user"
                        and event.get("channel_id") == api.channel_id):
                    content = event["content"]
                    message_id = event.get("message_id")

                    # Mark as read via API
                    await api.mark_read([message_id])

                    # Feed to the agent
                    await client.query(
                        f"New message from Zech:\n{content}"
                    )
```

**Advantages over polling:**
- Zero latency (instant delivery)
- No wasted requests
- Same transport the web UI uses
- Automatic reconnection via SSE spec

**Server-side requirement**: A `POST /api/session` endpoint that exchanges an Ed25519-signed request for a session cookie the agent can use to connect to the existing SSE stream.

### Phase 5: Auto-Send Responses

```python
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

def handle_message(message, api: AiChatAPI):
    if isinstance(message, ResultMessage):
        if message.result:
            asyncio.create_task(api.send_message(message.result))
    elif isinstance(message, AssistantMessage):
        text_parts = [
            block.text for block in message.content
            if isinstance(block, TextBlock)
        ]
        if text_parts:
            asyncio.create_task(api.send_message("\n".join(text_parts)))
```

## File Changes

| File | Action | Purpose |
|------|--------|---------|
| `aichat_agent.py` | **Create** | SDK wrapper script (optional substitute) |
| `aichat_api.py` | **Create** | Unified async API client |
| `pyproject.toml` | **Modify** | Add `claude-agent-sdk` dependency |
| `Claude.md` | **Modify** | Document SDK agent as an option |
| `relay_hook.py` | **Keep** | Still works for standard Claude Code |
| `aichat_send.py` | **Keep** | Standalone CLI tool |
| `aichat_unread.py` | **Keep** | Standalone CLI tool |
| `aichat_read.py` | **Keep** | Standalone CLI tool |

## Server-Side Changes

The existing Skrift SSE stream requires a session cookie. Rather than modifying the stream's auth, add a new API endpoint that exchanges an Ed25519-signed request for a session cookie, which the agent can then use to connect to the existing SSE stream.

| File | Change |
|------|--------|
| `controllers/aichat.py` | Add `POST /api/session` — Ed25519-authenticated endpoint that returns a session cookie for SSE access |

The flow:
1. Agent signs a `POST /api/session` request with Ed25519
2. Server validates signature, creates a session scoped to the channel
3. Returns `Set-Cookie` with the session
4. Agent uses that cookie to connect to the existing `/notifications/stream` SSE endpoint

## Configuration

The wrapper needs:
- `ANTHROPIC_API_KEY` — for the Agent SDK to call Claude's API
- AI.CHAT compound token — resolved from `~/.config/aichat/tokens.json`
- Working directory — determines which project the agent operates on

## Usage

```bash
# Start the SDK agent for the current project
uv run python3 aichat_agent.py

# Or with a specific initial prompt
uv run python3 aichat_agent.py "Fix the auth bug in login.py"

# Standard Claude Code still works as before (hooks + CLI scripts)
claude
```

## Benefits

1. **Instant messages**: SSE stream vs polling — zero latency on incoming messages
2. **Single process**: No shell hooks, no cron jobs, no separate scripts
3. **Reliable stop detection**: SDK `Stop` hook fires reliably
4. **Conversation continuity**: `ClaudeSDKClient` maintains session across messages
5. **Programmatic control**: Can interrupt, resume, change model, etc.
6. **Auto-response**: Agent automatically sends its responses to the chat
7. **Non-breaking**: Existing hook + CLI approach continues to work unchanged

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Agent SDK requires `ANTHROPIC_API_KEY` (direct API billing) | Budget cap via `max_budget_usd` option |
| Long-running process may crash | Wrap in supervisor/systemd, send error notification |
| Concurrent message handling | Queue incoming messages, process sequentially |
| SDK version churn | Pin version in pyproject.toml |
| Session endpoint needs server changes | Fallback to polling if session endpoint not yet deployed |

## Decisions

1. **Long-lived daemon** — the agent runs continuously, not restarted per-task
2. **`ClaudeSDKClient`** (continuous) for session continuity across messages
3. **Buffer messages mid-task** — incoming messages are queued and injected after the current task completes
4. **Final text only** — auto-send responses include just text, tool status is reported separately via hooks
5. **Session endpoint for SSE access** — new `POST /api/session` exchanges Ed25519 signature for a session cookie, then connect to the existing Skrift SSE stream
