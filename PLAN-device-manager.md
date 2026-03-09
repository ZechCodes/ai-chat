# AI.CHAT Device Manager — Implementation Plan

## Overview

Replace channel-level auth with **device-level auth**. Each device runs a **manager** process that authenticates via a device auth flow, receives commands over SSE, and launches/monitors **worker** processes (Agent SDK wrappers) for individual channels.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Server (zech.sh / Skrift)                               │
│                                                          │
│  ┌──────────────┐  ┌────────────────────┐  ┌──────────┐  │
│  │ Device Auth  │  │ Skrift Notif. SSE  │  │ Channel  │  │
│  │ /api/devices │  │ /notifications/    │  │ API      │  │
│  │              │  │  stream?since=...  │  │          │  │
│  └──────────────┘  └────────────────────┘  └──────────┘  │
└──────────────────────────────────────────────────────────┘
         │                  │                  │
         │ Device Auth      │ Timeseries       │ Worker API
         │                  │ Notifications    │
         │                  │ (commands)       │
┌────────┴──────────────────┴──────────────────┴───────────┐
│  Device (local machine)                                  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │  Manager Process (aichat-manager)                │    │
│  │  - Authenticates device via browser approval     │    │
│  │  - Gets session cookie via POST /api/session     │    │
│  │  - Connects to Skrift /notifications/stream      │    │
│  │  - Tracks last_event_ts, sends ?since= on reconnect  │
│  │  - Launches/stops/monitors workers               │    │
│  │  - Reports device status                         │    │
│  ├──────────────────────────────────────────────────┤    │
│  │  Worker 1 (aichat_agent.py)                      │    │
│  │  - Channel: ch-abc123                            │    │
│  │  - ClaudeSDKClient + SSE listener                │    │
│  ├──────────────────────────────────────────────────┤    │
│  │  Worker 2 (aichat_agent.py)                      │    │
│  │  - Channel: ch-def456                            │    │
│  │  - ClaudeSDKClient + SSE listener                │    │
│  └──────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

## Device Auth Flow

```
1. User runs: aichat-manager
2. Manager generates a device keypair (Ed25519)
3. Manager calls POST /api/devices/register
   → Server returns a device_code + auth_url
4. Manager prints: "Authorize this device: https://aichat.zech.sh/devices/authorize?code=XXXX"
   (or opens browser)
5. User visits URL, sees: "Trust device 'Zech's MacBook'? [Approve] [Deny]"
6. User clicks Approve → server marks device as trusted
7. Manager polls GET /api/devices/status?code=XXXX (or SSE)
   → Gets device_id + device_token on approval
8. Manager stores device_token locally (~/.config/aichat/device.json)
9. Subsequent starts skip auth, use stored token
```

## Manager SSE — Skrift Notification Stream

The manager uses **Skrift's existing notification SSE stream** rather than a custom endpoint. Commands are delivered as **timeseries notifications**, which gives us buffering and replay for free.

### Connection

```
1. Manager exchanges device Ed25519 auth for a session cookie:
   POST /api/session (device-level, returns session with device's owner user)

2. Manager connects to Skrift's notification stream:
   GET /notifications/stream?since={last_event_ts}
   Cookie: session cookie from step 1

3. Manager tracks last_event_ts from each received event
4. On reconnect, manager sends ?since=last_event_ts to replay missed events
```

This means:
- **No custom SSE endpoint** — reuse Skrift's battle-tested stream
- **Offline resilience** — timeseries notifications persist; manager replays on reconnect via `?since=`
- **No polling** — pure SSE for snappy realtime experience

### Command Types (delivered as timeseries notifications)

Commands are sent as Skrift notifications with `type: "aichat:device-command"`:

| Command | Payload | Action |
|---------|---------|--------|
| `worker:start` | `{channel_id, channel_token}` | Launch a new worker for the channel |
| `worker:stop` | `{channel_id}` | Stop the worker for the channel |
| `worker:restart` | `{channel_id}` | Restart the worker |
| `device:ping` | `{}` | Health check — manager responds with status |
| `device:update` | `{name}` | Update device display name locally |

### Manager → Server Status Updates

The manager reports back via API:

```
POST /api/devices/{device_id}/status
{
    "status": "online",
    "workers": [
        {"channel_id": "ch-abc", "status": "running", "uptime": 3600},
        {"channel_id": "ch-def", "status": "running", "uptime": 1200}
    ]
}
```

## Worker Lifecycle

```python
class WorkerManager:
    """Manages worker processes on this device."""

    workers: dict[str, WorkerProcess]  # channel_id → process

    async def start_worker(self, channel_id: str, channel_token: str):
        """Launch a worker subprocess for the given channel."""
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "aichat_agent.py",
            env={**os.environ, "AICHAT_PRIVATE_KEY": channel_token},
        )
        self.workers[channel_id] = WorkerProcess(proc, channel_id)

    async def stop_worker(self, channel_id: str):
        """Gracefully stop a worker."""
        worker = self.workers.pop(channel_id, None)
        if worker:
            worker.proc.terminate()
            await asyncio.wait_for(worker.proc.wait(), timeout=10)

    async def monitor(self):
        """Watch for crashed workers and restart them."""
        while True:
            for channel_id, worker in list(self.workers.items()):
                if worker.proc.returncode is not None:
                    log.warning("Worker %s crashed, restarting", channel_id)
                    await self.start_worker(channel_id, worker.token)
            await asyncio.sleep(5)
```

## Server-Side Changes

### New Models

```python
class AiChatDevice(Base):
    __tablename__ = "ai_chat_devices"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str]                          # "Zech's MacBook"
    public_key: Mapped[str]                    # Ed25519 public key (b64)
    owner_user_id: Mapped[UUID]                # FK to User
    status: Mapped[str] = mapped_column(default="offline")  # online/offline
    last_seen_at: Mapped[datetime | None]
    created_at: Mapped[datetime]
```

### New Endpoints

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| `POST` | `/api/devices/register` | None | Start device auth flow, returns device_code + auth_url |
| `GET` | `/api/devices/status` | device_code | Poll for auth completion |
| `GET` | `/devices/authorize` | Session | Web page to approve device |
| `POST` | `/devices/authorize` | Session + CSRF | Approve/deny device |
| `POST` | `/api/session` | Device token | Exchange device auth for session cookie (for Skrift SSE) |
| `POST` | `/api/devices/{id}/status` | Device token | Manager reports status |
| `POST` | `/api/devices/{id}/workers` | User session | Request new worker (creates channel + sends command) |
| `DELETE` | `/api/devices/{id}/workers/{channel_id}` | User session | Request worker stop |
| `PUT` | `/api/devices/{id}` | User session | Update device name |
| `DELETE` | `/api/devices/{id}` | User session | Delete device |

### Modified Endpoints

| Endpoint | Change |
|----------|--------|
| `GET /` (dashboard) | Group channels by device, show device sections |
| `POST /channels` | Associate channel with device_id |

### Dashboard Changes

The home page groups channels by device:

```
┌──────────────────────────────────────────┐
│  Zech's MacBook  ● Online    [✏️] [+ Task] │
│  ┌────────────────────────────────────┐  │
│  │ frontend-refactor    ● Working     │  │
│  │ api-migration        ○ Waiting  3  │  │
│  └────────────────────────────────────┘  │
│                                          │
│  Zech's Desktop  ○ Offline   [✏️] [+ Task] │
│  ┌────────────────────────────────────┐  │
│  │ data-pipeline        ○ Offline     │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
```

Each device section shows:
- Device name + online/offline status
- Edit button (rename or delete device)
- New Task button (creates channel + starts worker)
- List of channels/workers with status + unread badges

## Client-Side Files

| File | Action | Purpose |
|------|--------|---------|
| `aichat_manager.py` | **Create** | Manager daemon — auth, SSE, worker lifecycle |
| `aichat_device_auth.py` | **Create** | Device auth flow (keypair gen, registration, polling) |
| `aichat_agent.py` | **Keep** | Worker process (unchanged) |
| `aichat_api.py` | **Modify** | Add device API methods |
| `CLAUDE.md` | **Modify** | Document manager usage |

## Server-Side Files

| File | Action | Purpose |
|------|--------|---------|
| `models/ai_chat_device.py` | **Create** | Device model |
| `models/ai_chat_channel.py` | **Modify** | Add device_id FK |
| `controllers/aichat.py` | **Modify** | Add device endpoints, update dashboard |
| `templates/aichat_dashboard.html` | **Modify** | Device-grouped layout |
| `migrations/` | **Create** | Add devices table, channel.device_id column |

## Implementation Phases

### Phase 1: Device Model + Auth Flow
- Create `AiChatDevice` model + migration
- Add `device_id` FK to `AiChatChannel`
- Implement `/api/devices/register` and `/devices/authorize` endpoints
- Implement `aichat_device_auth.py` (keypair gen, registration, polling)
- Store device token in `~/.config/aichat/device.json`

### Phase 2: Manager Process
- Create `aichat_manager.py` with `WorkerManager` class
- Get session cookie via `POST /api/session` (device-level auth)
- Connect to Skrift `/notifications/stream?since=` for commands
- Track `last_event_ts` for reconnect replay
- Implement `worker:start`, `worker:stop`, `worker:restart` handlers
- Worker crash monitoring + auto-restart
- Status reporting to server

### Phase 3: Server Commands + API
- Send device commands as Skrift timeseries notifications
- Implement worker request endpoints (create channel + send command)
- Implement device status endpoint
- Add device CRUD (rename, delete)

### Phase 4: Dashboard Redesign
- Group channels by device on home page
- Device online/offline indicators
- "New Task" button per device (creates channel + requests worker)
- Edit device (rename/delete)
- Worker status per channel

### Phase 5: Polish ✓
- Device auth CLI UX (auto-open browser) ✓
- Graceful shutdown (manager sends offline status) ✓
- Worker health reporting (memory RSS + uptime in status) ✓
- Device key rotation (`device:rotate-key` command + `/api/device/rotate-key` endpoint) ✓

## Security Considerations

- Device keypairs are Ed25519, same as channel keys
- Device auth uses a short-lived device_code (expires in 10 min)
- Approval requires authenticated session (user must be logged in)
- Device tokens are stored locally with restricted permissions
- Workers inherit auth via channel tokens passed from manager
- Manager → server communication uses device-level Ed25519 signing
- Deleting a device revokes all its channel associations

## Decisions (Resolved)

1. **SSE stream**: Use Skrift's `/notifications/stream` with session cookie from `POST /api/session` — no custom device SSE endpoint
2. **New Task naming**: Sensible default name, prompt user to customize
3. **API keys**: Workers share the device's `ANTHROPIC_API_KEY` — manager passes it down
4. **Offline handling**: Show "offline" status, buffer commands as timeseries notifications, replay on reconnect via `?since=` (comes free from Skrift timeseries)
