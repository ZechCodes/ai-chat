"""Local SQLite database for AI.CHAT — stores messages, channels, and metadata.

This is the local source of truth. The server only relays encrypted blobs;
all plaintext content lives here.

Usage:
    db = LocalDB()
    await db.open()
    await db.save_message(channel_id, message_id, "claude", "Hello!", ...)
    messages = await db.get_messages(channel_id, limit=100)
    await db.close()
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

DEFAULT_DB_PATH = Path.home() / ".config" / "aichat" / "messages.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    content TEXT NOT NULL,
    user_id TEXT,
    attachments TEXT,
    read_by_claude_at TEXT,
    read_by_user_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_msg_channel_created ON messages(channel_id, created_at);
CREATE INDEX IF NOT EXISTS ix_msg_created_at ON messages(created_at);

CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    device_id TEXT,
    working_directory TEXT,
    additional_directories TEXT,
    archived INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class LocalDB:
    """Async SQLite wrapper for the local message store."""

    def __init__(self, path: Path = DEFAULT_DB_PATH):
        self.path = path
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.path))
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_SCHEMA)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        # Migration: drop channel_key_b64 column if it exists (SQLite 3.35+)
        try:
            cursor = await self._db.execute("PRAGMA table_info(channels)")
            cols = [row[1] for row in await cursor.fetchall()]
            if "channel_key_b64" in cols:
                await self._db.execute("ALTER TABLE channels DROP COLUMN channel_key_b64")
        except Exception:
            pass  # Old SQLite or column already gone
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("Database not open — call await db.open() first")
        return self._db

    # ------------------------------------------------------------------
    # Messages
    # ------------------------------------------------------------------

    async def save_message(
        self,
        channel_id: str,
        sender: str,
        content: str,
        *,
        message_id: str | None = None,
        user_id: str | None = None,
        attachments: list | None = None,
        read_by_claude_at: str | None = None,
        read_by_user_at: str | None = None,
        created_at: str | None = None,
    ) -> str:
        """Insert or replace a message. Returns the message ID."""
        msg_id = message_id or uuid.uuid4().hex
        now = _now()
        await self.db.execute(
            """INSERT OR REPLACE INTO messages
               (id, channel_id, sender, content, user_id, attachments,
                read_by_claude_at, read_by_user_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                msg_id,
                channel_id,
                sender,
                content,
                user_id,
                json.dumps(attachments) if attachments else None,
                read_by_claude_at,
                read_by_user_at,
                created_at or now,
                now,
            ),
        )
        await self.db.commit()
        return msg_id

    async def get_messages(
        self,
        channel_id: str,
        *,
        limit: int = 100,
        before_id: str | None = None,
    ) -> list[dict]:
        """Get messages for a channel, newest first. Optionally paginate with before_id."""
        if before_id:
            cursor = await self.db.execute(
                """SELECT * FROM messages
                   WHERE channel_id = ? AND created_at < (
                       SELECT created_at FROM messages WHERE id = ?
                   )
                   ORDER BY created_at DESC LIMIT ?""",
                (channel_id, before_id, limit),
            )
        else:
            cursor = await self.db.execute(
                """SELECT * FROM messages
                   WHERE channel_id = ?
                   ORDER BY created_at DESC LIMIT ?""",
                (channel_id, limit),
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in reversed(rows)]  # Return in chronological order

    async def get_messages_by_ids(
        self,
        channel_id: str,
        message_ids: list[str],
    ) -> list[dict]:
        """Get specific messages by ID for a channel."""
        if not message_ids:
            return []
        placeholders = ",".join("?" for _ in message_ids)
        cursor = await self.db.execute(
            f"""SELECT * FROM messages
               WHERE channel_id = ? AND id IN ({placeholders})
               ORDER BY created_at ASC""",
            [channel_id] + message_ids,
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def mark_read_by_claude(self, message_ids: list[str]) -> None:
        """Mark messages as read by the agent."""
        now = _now()
        await self.db.executemany(
            "UPDATE messages SET read_by_claude_at = ?, updated_at = ? WHERE id = ?",
            [(now, now, mid) for mid in message_ids],
        )
        await self.db.commit()

    async def mark_read_by_user(self, message_ids: list[str]) -> None:
        """Mark messages as read by the user."""
        now = _now()
        await self.db.executemany(
            "UPDATE messages SET read_by_user_at = ?, updated_at = ? WHERE id = ?",
            [(now, now, mid) for mid in message_ids],
        )
        await self.db.commit()

    # ------------------------------------------------------------------
    # Channels
    # ------------------------------------------------------------------

    async def save_channel(
        self,
        channel_id: str,
        name: str,
        *,
        device_id: str | None = None,
        working_directory: str | None = None,
        additional_directories: list[str] | None = None,
    ) -> None:
        """Insert or update a channel."""
        now = _now()
        await self.db.execute(
            """INSERT INTO channels (id, name, device_id, working_directory,
                   additional_directories, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   name = excluded.name,
                   device_id = excluded.device_id,
                   working_directory = COALESCE(excluded.working_directory, channels.working_directory),
                   additional_directories = COALESCE(excluded.additional_directories, channels.additional_directories),
                   updated_at = excluded.updated_at""",
            (
                channel_id,
                name,
                device_id,
                working_directory,
                json.dumps(additional_directories) if additional_directories else None,
                now,
                now,
            ),
        )
        await self.db.commit()

    async def get_channel(self, channel_id: str) -> dict | None:
        cursor = await self.db.execute(
            "SELECT * FROM channels WHERE id = ?", (channel_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def list_channels(self, *, include_archived: bool = False) -> list[dict]:
        if include_archived:
            cursor = await self.db.execute(
                "SELECT * FROM channels ORDER BY created_at"
            )
        else:
            cursor = await self.db.execute(
                "SELECT * FROM channels WHERE archived = 0 ORDER BY created_at"
            )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def archive_channel(self, channel_id: str) -> None:
        now = _now()
        await self.db.execute(
            "UPDATE channels SET archived = 1, updated_at = ? WHERE id = ?",
            (now, channel_id),
        )
        await self.db.commit()

    # ------------------------------------------------------------------
    # Sync state
    # ------------------------------------------------------------------

    async def get_sync_state(self, key: str) -> str | None:
        cursor = await self.db.execute(
            "SELECT value FROM sync_state WHERE key = ?", (key,)
        )
        row = await cursor.fetchone()
        return row["value"] if row else None

    async def set_sync_state(self, key: str, value: str) -> None:
        await self.db.execute(
            "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
            (key, value),
        )
        await self.db.commit()
