"""Tests for the device manager."""

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from aichat_manager import (
    DeviceManager,
    WorkerProcess,
    parse_device_command,
)


class TestParseDeviceCommand:
    def test_parses_worker_start(self):
        event = {
            "event_type": "aichat:device-command",
            "command": "worker:start",
            "payload": {"channel_id": "ch-abc", "channel_token": "tok-123"},
        }
        result = parse_device_command(event)
        assert result is not None
        assert result["command"] == "worker:start"
        assert result["payload"]["channel_id"] == "ch-abc"

    def test_parses_worker_stop(self):
        event = {
            "event_type": "aichat:device-command",
            "command": "worker:stop",
            "payload": {"channel_id": "ch-abc"},
        }
        result = parse_device_command(event)
        assert result is not None
        assert result["command"] == "worker:stop"

    def test_ignores_non_command_events(self):
        event = {"event_type": "aichat:message", "content": "hello"}
        assert parse_device_command(event) is None

    def test_ignores_missing_type(self):
        event = {"command": "worker:start"}
        assert parse_device_command(event) is None

    def test_parses_device_ping(self):
        event = {
            "event_type": "aichat:device-command",
            "command": "device:ping",
            "payload": {},
        }
        result = parse_device_command(event)
        assert result["command"] == "device:ping"


class TestWorkerProcess:
    def test_stores_metadata(self):
        proc = MagicMock()
        wp = WorkerProcess(proc=proc, channel_id="ch-1", channel_token="tok-1")
        assert wp.channel_id == "ch-1"
        assert wp.channel_token == "tok-1"
        assert wp.proc is proc


class TestDeviceManager:
    @pytest.fixture
    def manager(self):
        key = Ed25519PrivateKey.generate()
        key_b64 = base64.b64encode(key.private_bytes_raw()).decode()
        return DeviceManager(
            device_id="dev-123",
            private_key_b64=key_b64,
            base_url="https://aichat.zech.sh",
        )

    def test_init(self, manager):
        assert manager.device_id == "dev-123"
        assert manager.workers == {}

    @pytest.mark.asyncio
    async def test_start_worker_launches_subprocess(self, manager):
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.wait = AsyncMock()
        with patch("aichat_manager.asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
            await manager.start_worker("ch-abc", "tok-abc")
            assert "ch-abc" in manager.workers
            assert manager.workers["ch-abc"].proc is mock_proc
            assert manager.workers["ch-abc"].channel_token == "tok-abc"
            mock_exec.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_worker_stops_existing_first(self, manager):
        old_proc = MagicMock()
        old_proc.returncode = None
        old_proc.wait = AsyncMock()
        manager.workers["ch-abc"] = WorkerProcess(proc=old_proc, channel_id="ch-abc", channel_token="old-tok")

        new_proc = MagicMock()
        new_proc.returncode = None
        new_proc.wait = AsyncMock()
        with patch("aichat_manager.asyncio.create_subprocess_exec", return_value=new_proc):
            await manager.start_worker("ch-abc", "new-tok")
            old_proc.terminate.assert_called_once()
            assert manager.workers["ch-abc"].channel_token == "new-tok"

    @pytest.mark.asyncio
    async def test_stop_worker(self, manager):
        proc = MagicMock()
        proc.returncode = None
        proc.wait = AsyncMock()
        manager.workers["ch-abc"] = WorkerProcess(proc=proc, channel_id="ch-abc", channel_token="tok")

        await manager.stop_worker("ch-abc")
        proc.terminate.assert_called_once()
        assert "ch-abc" not in manager.workers

    @pytest.mark.asyncio
    async def test_stop_nonexistent_worker(self, manager):
        # Should not raise
        await manager.stop_worker("ch-nonexistent")

    @pytest.mark.asyncio
    async def test_handle_command_worker_start(self, manager):
        mock_proc = MagicMock()
        mock_proc.returncode = None
        mock_proc.wait = AsyncMock()
        with patch("aichat_manager.asyncio.create_subprocess_exec", return_value=mock_proc):
            await manager.handle_command({
                "command": "worker:start",
                "payload": {"channel_id": "ch-1", "channel_token": "tok-1"},
            })
            assert "ch-1" in manager.workers

    @pytest.mark.asyncio
    async def test_handle_command_worker_stop(self, manager):
        proc = MagicMock()
        proc.returncode = None
        proc.wait = AsyncMock()
        manager.workers["ch-1"] = WorkerProcess(proc=proc, channel_id="ch-1", channel_token="tok-1")

        await manager.handle_command({
            "command": "worker:stop",
            "payload": {"channel_id": "ch-1"},
        })
        assert "ch-1" not in manager.workers

    @pytest.mark.asyncio
    async def test_handle_command_worker_restart(self, manager):
        old_proc = MagicMock()
        old_proc.returncode = None
        old_proc.wait = AsyncMock()
        manager.workers["ch-1"] = WorkerProcess(proc=old_proc, channel_id="ch-1", channel_token="tok-1")

        new_proc = MagicMock()
        new_proc.returncode = None
        new_proc.wait = AsyncMock()
        with patch("aichat_manager.asyncio.create_subprocess_exec", return_value=new_proc):
            await manager.handle_command({
                "command": "worker:restart",
                "payload": {"channel_id": "ch-1"},
            })
            old_proc.terminate.assert_called_once()
            assert manager.workers["ch-1"].proc is new_proc

    def test_get_status(self, manager):
        proc = MagicMock()
        proc.returncode = None
        proc.pid = 12345
        manager.workers["ch-1"] = WorkerProcess(proc=proc, channel_id="ch-1", channel_token="tok-1")

        with patch.object(manager, "_get_worker_memory_mb", return_value=42.5):
            status = manager.get_status()
        assert status["status"] == "online"
        assert status["device_id"] == "dev-123"
        assert len(status["workers"]) == 1
        assert status["workers"][0]["channel_id"] == "ch-1"
        assert status["workers"][0]["status"] == "running"
        assert status["workers"][0]["memory_mb"] == 42.5

    def test_get_status_no_memory_info(self, manager):
        proc = MagicMock()
        proc.returncode = None
        proc.pid = 12345
        manager.workers["ch-1"] = WorkerProcess(proc=proc, channel_id="ch-1", channel_token="tok-1")

        with patch.object(manager, "_get_worker_memory_mb", return_value=None):
            status = manager.get_status()
        assert "memory_mb" not in status["workers"][0]

    def test_get_status_crashed_worker(self, manager):
        proc = MagicMock()
        proc.returncode = 1  # crashed
        manager.workers["ch-1"] = WorkerProcess(proc=proc, channel_id="ch-1", channel_token="tok-1")

        status = manager.get_status()
        assert status["workers"][0]["status"] == "crashed"
        assert "memory_mb" not in status["workers"][0]

    def test_get_status_adopted_worker(self, manager):
        manager.workers["ch-1"] = WorkerProcess(
            proc=None,
            channel_id="ch-1",
            channel_token="",
            adopted=True,
            adopted_pid=43210,
        )
        with patch("aichat_manager.os.kill") as mock_kill:
            mock_kill.return_value = None
            with patch.object(manager, "_get_worker_memory_mb", return_value=12.3):
                status = manager.get_status()
        assert status["workers"][0]["status"] == "running"
        assert status["workers"][0]["memory_mb"] == 12.3

    @pytest.mark.asyncio
    async def test_report_status(self, manager):
        with patch.object(manager, "_ws_request", AsyncMock(return_value={"ok": True})) as mock_ws:
            await manager.report_status()
        mock_ws.assert_awaited_once()
        payload = mock_ws.await_args.args[0]
        assert payload["type"] == "report_status"
        assert payload["device_id"] == "dev-123"

    @pytest.mark.asyncio
    async def test_sync_channels_starts_missing(self, manager):
        with patch.object(manager, "_ws_request", AsyncMock(return_value={"channels": [
                {"id": "ch-1", "name": "Task 1"},
                {"id": "ch-2", "name": "Task 2"},
            ]})):
            new_proc = MagicMock()
            new_proc.returncode = None
            new_proc.wait = AsyncMock()
            with patch("aichat_manager.asyncio.create_subprocess_exec", return_value=new_proc):
                await manager.sync_channels()
        assert "ch-1" in manager.workers
        assert "ch-2" in manager.workers

    @pytest.mark.asyncio
    async def test_sync_channels_stops_removed(self, manager):
        proc = MagicMock()
        proc.returncode = None
        proc.wait = AsyncMock()
        manager.workers["ch-old"] = WorkerProcess(proc=proc, channel_id="ch-old", channel_token="")

        with patch.object(manager, "_ws_request", AsyncMock(return_value={"channels": []})):
            await manager.sync_channels()
        assert "ch-old" not in manager.workers
        proc.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_monitor_detects_crashed_worker(self, manager):
        crashed_proc = MagicMock()
        crashed_proc.returncode = 1  # crashed
        crashed_proc.wait = AsyncMock()

        new_proc = MagicMock()
        new_proc.returncode = None
        new_proc.wait = AsyncMock()

        manager.workers["ch-1"] = WorkerProcess(
            proc=crashed_proc, channel_id="ch-1", channel_token="tok-1"
        )

        with patch("aichat_manager.asyncio.create_subprocess_exec", return_value=new_proc):
            # Run one iteration of monitor
            await manager._check_workers()
            assert manager.workers["ch-1"].proc is new_proc
