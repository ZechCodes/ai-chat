"""Tests for SDK hook functions."""

import json

import httpx
import pytest
import respx

from aichat_api import AiChatAPI
from aichat_hooks import make_pre_tool_hook, make_post_tool_hook, make_stop_hook


@pytest.fixture
def api(env_with_token):
    return AiChatAPI()


class TestPreToolHook:
    @respx.mock
    @pytest.mark.asyncio
    async def test_reports_active_status(self, api):
        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        hook = make_pre_tool_hook(api, {})
        input_data = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Read",
            "tool_input": {"file_path": "/tmp/test.py"},
            "tool_use_id": "tu-1",
            "session_id": "sess-1",
            "transcript_path": "/tmp/transcript",
            "cwd": "/tmp",
            "permission_mode": None,
        }
        result = await hook(input_data, "tu-1", None)
        assert result == {"continue_": True}
        assert route.called
        body = json.loads(route.calls[0].request.content)
        assert body["status"] == "active"
        assert body["tool"] == "Read"
        assert "Reading" in body["description"]

    @respx.mock
    @pytest.mark.asyncio
    async def test_skips_own_api_calls(self, api):
        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        hook = make_pre_tool_hook(api, {})
        input_data = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": {"command": "uv run python3 aichat_send.py hello"},
            "tool_use_id": "tu-2",
            "session_id": "sess-1",
            "transcript_path": "/tmp/transcript",
            "cwd": "/tmp",
            "permission_mode": None,
        }
        result = await hook(input_data, "tu-2", None)
        assert result == {"continue_": True}
        assert not route.called

    @respx.mock
    @pytest.mark.asyncio
    async def test_describes_bash_command(self, api):
        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        hook = make_pre_tool_hook(api, {})
        input_data = {
            "hook_event_name": "PreToolUse",
            "tool_name": "Bash",
            "tool_input": {"command": "git status"},
            "tool_use_id": "tu-3",
            "session_id": "sess-1",
            "transcript_path": "/tmp/transcript",
            "cwd": "/tmp",
            "permission_mode": None,
        }
        await hook(input_data, "tu-3", None)
        body = json.loads(route.calls[0].request.content)
        assert "git status" in body["description"]


class TestPostToolHook:
    @respx.mock
    @pytest.mark.asyncio
    async def test_reports_done_status(self, api):
        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        hook = make_post_tool_hook(api)
        input_data = {
            "hook_event_name": "PostToolUse",
            "tool_name": "Read",
            "tool_input": {},
            "tool_response": "file contents...",
            "tool_use_id": "tu-1",
            "session_id": "sess-1",
            "transcript_path": "/tmp/transcript",
            "cwd": "/tmp",
            "permission_mode": None,
        }
        result = await hook(input_data, "tu-1", None)
        assert result == {}
        body = json.loads(route.calls[0].request.content)
        assert body["status"] == "done"
        assert body["tool"] == "Read"


class TestStopHook:
    @respx.mock
    @pytest.mark.asyncio
    async def test_reports_idle_status(self, api):
        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        hook = make_stop_hook(api)
        input_data = {
            "hook_event_name": "Stop",
            "stop_hook_active": True,
            "session_id": "sess-1",
            "transcript_path": "/tmp/transcript",
            "cwd": "/tmp",
            "permission_mode": None,
        }
        result = await hook(input_data, None, None)
        assert result == {}
        body = json.loads(route.calls[0].request.content)
        assert body["status"] == "idle"
