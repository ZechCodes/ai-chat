## Messaging Tools

Tokens are resolved automatically from `~/.config/aichat/tokens.json` by working directory.

### `aichat-send` — Send a message to Zech
```
aichat-send "your message here"
```
Use this to:
- Announce startup and task completion
- Send periodic status updates
- Ask Zech questions or report issues

### `aichat-unread` — Read unread messages from Zech
```
aichat-unread
```
Use this to check for new instructions or replies. Run on startup and periodically (set up `/loop 1m check for new messages`).

### `aichat-read` — Read recent message history
```
aichat-read [limit]
```
Use this to review conversation context. Optional `limit` argument controls how many messages to fetch.

### `aichat-register` — Register a token for a directory
```
aichat-register TOKEN [DIRECTORY]
```
Register a compound token for a working directory. If directory is omitted, uses cwd.

## Agent SDK Wrapper (Optional)

An alternative to the hook + CLI script approach. Runs Claude Code as an SDK-managed agent with real-time SSE message delivery.

```bash
# Start the SDK agent for the current project
uv run python3 aichat_agent.py

# With an initial prompt
uv run python3 aichat_agent.py "Fix the auth bug"
```

## Device Manager Plan

See [PLAN-device-manager.md](./PLAN-device-manager.md) for the implementation plan for device-level auth with a manager/worker architecture. The manager authenticates devices, receives commands via SSE, and launches/monitors Agent SDK workers per channel.

## Rules

Never use plan mode. It is imperative that you do not use plan mode. Zech may
not be available to approve your plan and you'll be unable to communicate with
him while you're awaiting approval.

When you first start up and when you finish a task use aichat-send to
tell Zech. Periodically send updates to Zech and use aichat-unread to
check for new messages.
