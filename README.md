# AI Chat — Claude Code Messaging Toolkit

A lightweight toolkit that lets [Claude Code](https://claude.ai/claude-code) agents communicate with you through a real-time chat interface. Agents can send messages, check for replies, and relay tool-use status — all through a simple set of CLI commands.

## How It Works

1. **You** create a channel on the AI Chat dashboard and get a compound token
2. **Register** the token for any project directory
3. **Claude Code** uses the CLI commands to send/receive messages
4. **Global hooks** automatically relay tool-use activity (what Claude is reading, editing, running) to the chat UI in real time

## Setup

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager

### Install

```bash
git clone https://github.com/ZechCodes/ai-chat.git ~/Projects/ai-chat
cd ~/Projects/ai-chat
uv sync
```

### Create CLI Wrappers

Add these to a directory on your `$PATH` (e.g. `~/.local/bin/`):

```bash
# aichat-send
#!/bin/sh
exec uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/aichat_send.py "$@"

# aichat-unread
#!/bin/sh
exec uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/aichat_unread.py "$@"

# aichat-read
#!/bin/sh
exec uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/aichat_read.py "$@"

# aichat-register
#!/bin/sh
exec uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/register_token.py "$@"
```

Make them executable:

```bash
chmod +x ~/.local/bin/aichat-*
```

### Register a Token

After creating a channel on the AI Chat dashboard, register the token for a project:

```bash
aichat-register <TOKEN> /path/to/project
```

Tokens are stored in `~/.config/aichat/tokens.json`, keyed by directory path. Scripts automatically resolve the token for the current working directory.

## CLI Commands

### `aichat-send` — Send a message

```bash
aichat-send "Hello from Claude!"
aichat-send "Task complete. Deployed to production."
```

### `aichat-unread` — Check for unread messages

```bash
aichat-unread
# [2026-03-08T17:22:51] Please update the API endpoint
```

### `aichat-read` — Read recent message history

```bash
aichat-read        # Last 10 messages
aichat-read 50     # Last 50 messages
```

### `aichat-register` — Register a token for a directory

```bash
aichat-register TOKEN                    # Register for current directory
aichat-register TOKEN /path/to/project   # Register for specific directory
```

## Global Hooks

The toolkit includes `relay_hook.py` which relays Claude Code's activity to the chat UI. Add these hooks to `~/.claude/settings.json` to enable them globally:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "matcher": "",
        "hooks": [{ "type": "command", "command": "uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/relay_hook.py prompt_submit" }]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "",
        "hooks": [{ "type": "command", "command": "uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/relay_hook.py pre_tool_use" }]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "",
        "hooks": [{ "type": "command", "command": "uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/relay_hook.py post_tool_use" }]
      }
    ],
    "SubagentStart": [
      {
        "matcher": "",
        "hooks": [{ "type": "command", "command": "uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/relay_hook.py subagent_start" }]
      }
    ],
    "SubagentStop": [
      {
        "matcher": "",
        "hooks": [{ "type": "command", "command": "uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/relay_hook.py subagent_stop" }]
      }
    ],
    "Stop": [
      {
        "matcher": "",
        "hooks": [{ "type": "command", "command": "uv run --project ~/Projects/ai-chat python3 ~/Projects/ai-chat/relay_hook.py stop" }]
      }
    ]
  }
}
```

Hooks automatically no-op for projects that don't have a registered token.

## Claude Code Integration

Add this to your project's `CLAUDE.md` to instruct agents to use the messaging tools:

```markdown
## Messaging Tools

### `aichat-send` — Send a message to Zech
aichat-send "your message here"

Use this to announce startup, send status updates, and report issues.

### `aichat-unread` — Read unread messages
aichat-unread

Check for new instructions. Run on startup and periodically with `/loop 1m aichat-unread`.

### `aichat-read` — Read recent history
aichat-read [limit]
```

## Architecture

- **`aichat_auth.py`** — Ed25519 request signing and token resolution (env var or `~/.config/aichat/tokens.json`)
- **`aichat_send.py`** — Send messages to the chat API
- **`aichat_unread.py`** — Fetch and mark unread messages
- **`aichat_read.py`** — Read recent message history
- **`register_token.py`** — Register compound tokens for directories
- **`relay_hook.py`** — Claude Code hook that relays tool-use events to the chat UI

## Token Format

Tokens are compound tokens containing an Ed25519 private key, channel ID, and HMAC signature — base64url-encoded as a single string. They are generated by the server when creating a channel and shown once.

## Security

- All API requests are signed with Ed25519 (timestamp + method + path)
- Timestamp drift limited to 60 seconds
- Rate limited: 60 reads/min, 60 writes/min per channel
- Tokens never leave the local machine
