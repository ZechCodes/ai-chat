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

## Rules

Never use plan mode. It is imperative that you do not use plan mode. Zech may
not be available to approve your plan and you'll be unable to communicate with
him while you're awaiting approval.

When you first start up and when you finish a task use aichat-send to
tell Zech. Periodically send updates to Zech and use aichat-unread to
check for new messages.
