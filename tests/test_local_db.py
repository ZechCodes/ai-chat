"""Tests for aichat_db.LocalDB — local SQLite message store."""

from __future__ import annotations

import pytest
import pytest_asyncio
from pathlib import Path

from aichat_db import LocalDB


@pytest_asyncio.fixture
async def db(tmp_path: Path):
    """Create a temporary LocalDB for testing."""
    local_db = LocalDB(path=tmp_path / "test.db")
    await local_db.open()
    yield local_db
    await local_db.close()


@pytest.mark.asyncio
async def test_save_and_get_messages(db: LocalDB):
    channel_id = "chan-1"
    msg_id = await db.save_message(channel_id, "claude", "Hello!", message_id="msg-1")
    assert msg_id == "msg-1"

    messages = await db.get_messages(channel_id)
    assert len(messages) == 1
    assert messages[0]["content"] == "Hello!"
    assert messages[0]["sender"] == "claude"


@pytest.mark.asyncio
async def test_messages_returned_in_chronological_order(db: LocalDB):
    channel_id = "chan-1"
    await db.save_message(channel_id, "user", "First", message_id="m1", created_at="2026-01-01T00:00:00.000Z")
    await db.save_message(channel_id, "claude", "Second", message_id="m2", created_at="2026-01-01T00:01:00.000Z")
    await db.save_message(channel_id, "user", "Third", message_id="m3", created_at="2026-01-01T00:02:00.000Z")

    messages = await db.get_messages(channel_id)
    assert [m["id"] for m in messages] == ["m1", "m2", "m3"]


@pytest.mark.asyncio
async def test_pagination_with_before_id(db: LocalDB):
    channel_id = "chan-1"
    for i in range(5):
        await db.save_message(
            channel_id, "claude", f"msg-{i}",
            message_id=f"m{i}",
            created_at=f"2026-01-01T00:0{i}:00.000Z",
        )

    # Get messages before m3 (should return m0, m1, m2)
    messages = await db.get_messages(channel_id, before_id="m3", limit=10)
    assert [m["id"] for m in messages] == ["m0", "m1", "m2"]

    # With limit
    messages = await db.get_messages(channel_id, before_id="m3", limit=2)
    assert [m["id"] for m in messages] == ["m1", "m2"]


@pytest.mark.asyncio
async def test_channel_isolation(db: LocalDB):
    await db.save_message("chan-1", "claude", "In channel 1")
    await db.save_message("chan-2", "claude", "In channel 2")

    msgs1 = await db.get_messages("chan-1")
    msgs2 = await db.get_messages("chan-2")
    assert len(msgs1) == 1
    assert len(msgs2) == 1
    assert msgs1[0]["content"] == "In channel 1"
    assert msgs2[0]["content"] == "In channel 2"


@pytest.mark.asyncio
async def test_mark_read_by_claude(db: LocalDB):
    await db.save_message("chan-1", "user", "Hello", message_id="m1")
    await db.mark_read_by_claude(["m1"])

    messages = await db.get_messages("chan-1")
    assert messages[0]["read_by_claude_at"] is not None


@pytest.mark.asyncio
async def test_mark_read_by_user(db: LocalDB):
    await db.save_message("chan-1", "claude", "Hello", message_id="m1")
    await db.mark_read_by_user(["m1"])

    messages = await db.get_messages("chan-1")
    assert messages[0]["read_by_user_at"] is not None


@pytest.mark.asyncio
async def test_save_and_get_channel(db: LocalDB):
    await db.save_channel("chan-1", "Test Channel", device_id="dev-1", working_directory="/tmp")
    channel = await db.get_channel("chan-1")
    assert channel is not None
    assert channel["name"] == "Test Channel"
    assert channel["working_directory"] == "/tmp"


@pytest.mark.asyncio
async def test_channel_upsert_preserves_key(db: LocalDB):
    await db.save_channel("chan-1", "Test", channel_key_b64="secret-key")
    # Update name without providing key
    await db.save_channel("chan-1", "Updated Name")
    channel = await db.get_channel("chan-1")
    assert channel["name"] == "Updated Name"
    assert channel["channel_key_b64"] == "secret-key"


@pytest.mark.asyncio
async def test_list_channels_excludes_archived(db: LocalDB):
    await db.save_channel("chan-1", "Active")
    await db.save_channel("chan-2", "Archived")
    await db.archive_channel("chan-2")

    channels = await db.list_channels()
    assert len(channels) == 1
    assert channels[0]["id"] == "chan-1"

    all_channels = await db.list_channels(include_archived=True)
    assert len(all_channels) == 2


@pytest.mark.asyncio
async def test_sync_state(db: LocalDB):
    assert await db.get_sync_state("test_key") is None
    await db.set_sync_state("test_key", "test_value")
    assert await db.get_sync_state("test_key") == "test_value"
    # Overwrite
    await db.set_sync_state("test_key", "new_value")
    assert await db.get_sync_state("test_key") == "new_value"


@pytest.mark.asyncio
async def test_message_upsert(db: LocalDB):
    """INSERT OR REPLACE should update existing messages."""
    await db.save_message("chan-1", "claude", "Original", message_id="m1")
    await db.save_message("chan-1", "claude", "Updated", message_id="m1")

    messages = await db.get_messages("chan-1")
    assert len(messages) == 1
    assert messages[0]["content"] == "Updated"
