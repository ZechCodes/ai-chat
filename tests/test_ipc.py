"""Tests for IPC server/client behavior."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from aichat_ipc import (
    IPCClient,
    IPCServer,
    REGISTER_ROLE_AUX,
    REGISTER_ROLE_PRIMARY,
)


def _socket_path() -> str:
    return f"/tmp/aichat-ipc-test-{uuid.uuid4().hex[:10]}.sock"


async def _send_to_worker_with_retry(server: IPCServer, channel_id: str, message: dict) -> bool:
    for _ in range(20):
        if await server.send_to_worker(channel_id, message):
            return True
        await asyncio.sleep(0.01)
    return False


@pytest.mark.asyncio
async def test_request_before_connect_does_not_leak_pending():
    client = IPCClient("/tmp/does-not-exist.sock", "ch-1")
    with pytest.raises(ConnectionError):
        await client._request({"type": "send_message"})
    assert client._pending == {}


@pytest.mark.asyncio
async def test_fire_and_forget_before_connect_fails_cleanly():
    client = IPCClient("/tmp/does-not-exist.sock", "ch-1")
    with pytest.raises(ConnectionError):
        await client._fire_and_forget({"type": "tool_status"})


@pytest.mark.asyncio
async def test_round_trip_request_response():
    socket_path = _socket_path()

    async def on_request(channel_id: str, msg: dict) -> dict:
        return {"channel_id": channel_id, "type": msg.get("type")}

    server = IPCServer(socket_path, on_request=on_request)
    await server.start()
    client = IPCClient(socket_path, "ch-1")
    await client.connect()

    try:
        result = await client.send_event("plan:enter")
        assert result["channel_id"] == "ch-1"
        assert result["type"] == "send_event"
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_auxiliary_client_does_not_receive_worker_events():
    socket_path = _socket_path()

    async def on_request(channel_id: str, msg: dict) -> dict:
        return {}

    server = IPCServer(socket_path, on_request=on_request)
    await server.start()

    worker = IPCClient(socket_path, "ch-1", role=REGISTER_ROLE_PRIMARY)
    aux = IPCClient(socket_path, "ch-1", role=REGISTER_ROLE_AUX)
    await worker.connect()
    await aux.connect()

    worker_events: asyncio.Queue[dict] = asyncio.Queue()
    aux_events: asyncio.Queue[dict] = asyncio.Queue()

    async def on_worker_event(msg: dict) -> None:
        await worker_events.put(msg)

    async def on_aux_event(msg: dict) -> None:
        await aux_events.put(msg)

    worker.on_event(on_worker_event)
    aux.on_event(on_aux_event)

    try:
        sent = await _send_to_worker_with_retry(
            server, "ch-1", {"type": "message", "content": "hi"}
        )
        assert sent is True

        event = await asyncio.wait_for(worker_events.get(), timeout=0.5)
        assert event["type"] == "message"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(aux_events.get(), timeout=0.2)
    finally:
        await worker.close()
        await aux.close()
        await server.stop()


@pytest.mark.asyncio
async def test_new_primary_registration_replaces_old_primary():
    socket_path = _socket_path()

    async def on_request(channel_id: str, msg: dict) -> dict:
        return {}

    server = IPCServer(socket_path, on_request=on_request)
    await server.start()

    old_worker = IPCClient(socket_path, "ch-1", role=REGISTER_ROLE_PRIMARY)
    new_worker = IPCClient(socket_path, "ch-1", role=REGISTER_ROLE_PRIMARY)
    await old_worker.connect()
    await new_worker.connect()

    old_events: asyncio.Queue[dict] = asyncio.Queue()
    new_events: asyncio.Queue[dict] = asyncio.Queue()

    async def on_old_event(msg: dict) -> None:
        await old_events.put(msg)

    async def on_new_event(msg: dict) -> None:
        await new_events.put(msg)

    old_worker.on_event(on_old_event)
    new_worker.on_event(on_new_event)

    try:
        sent = await _send_to_worker_with_retry(
            server, "ch-1", {"type": "message", "content": "new-primary"}
        )
        assert sent is True

        event = await asyncio.wait_for(new_events.get(), timeout=0.5)
        assert event["content"] == "new-primary"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(old_events.get(), timeout=0.2)
    finally:
        await old_worker.close()
        await new_worker.close()
        await server.stop()
