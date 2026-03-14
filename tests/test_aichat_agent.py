"""Tests for the agent wrapper."""

import asyncio
import json
from unittest.mock import MagicMock

import httpx
import pytest
import respx

import aichat_agent
from aichat_api import AiChatAPI
from aichat_agent import (
    _await_next_message_or_listener_failure,
    _send_startup_status,
    build_agent_options,
    handle_response_message,
    listen_ipc,
    listen_sse,
    run_agent,
)
from aichat_interactions import InteractionManager


@pytest.fixture
def api(env_with_token):
    return AiChatAPI()


@pytest.fixture
def interactions():
    return InteractionManager()


@pytest.fixture
def build_inputs():
    import asyncio

    return {
        "message_queue": asyncio.Queue(),
        "skipped_messages": [],
        "hook_state": {
            "last_send_time": 0,
            "last_unread_notify": 0,
            "last_silence_remind": 0,
            "last_silence_remind_bg": 0,
            "working": False,
        },
    }


class TestBuildAgentOptions:
    def test_returns_options_with_hooks(self, api, interactions, build_inputs):
        options = build_agent_options(api, interactions, **build_inputs)
        assert options.hooks is not None
        assert "PreToolUse" in options.hooks
        assert "PostToolUse" in options.hooks
        assert "Stop" in options.hooks

    def test_sets_permission_mode(self, api, interactions, build_inputs):
        options = build_agent_options(api, interactions, **build_inputs)
        assert options.permission_mode == "plan"

    def test_sets_can_use_tool(self, api, interactions, build_inputs):
        options = build_agent_options(api, interactions, **build_inputs)
        assert options.can_use_tool is not None

    def test_loads_project_settings(self, api, interactions, build_inputs):
        options = build_agent_options(api, interactions, **build_inputs)
        assert options.setting_sources is not None
        assert "project" in options.setting_sources


class TestHandleResponseMessage:
    @respx.mock
    @pytest.mark.asyncio
    async def test_skips_result_message(self, api):
        from claude_agent_sdk import ResultMessage

        route = respx.post("https://test.example.com/api/messages").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        msg = MagicMock(spec=ResultMessage)
        msg.result = "Task completed successfully."
        await handle_response_message(msg, api)
        assert not route.called

    @respx.mock
    @pytest.mark.asyncio
    async def test_sends_assistant_text_as_tool_status(self, api):
        from claude_agent_sdk import AssistantMessage, TextBlock

        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        text_block = MagicMock(spec=TextBlock)
        text_block.text = "Here is my response."
        msg = MagicMock(spec=AssistantMessage)
        msg.content = [text_block]
        await handle_response_message(msg, api)
        assert route.called
        body = json.loads(route.calls[0].request.content)
        assert body["status"] == "active"
        assert body["tool"] == "reasoning"
        assert body["description"] == "Here is my response."

    @respx.mock
    @pytest.mark.asyncio
    async def test_skips_non_text_blocks(self, api):
        from claude_agent_sdk import AssistantMessage, ToolUseBlock

        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        tool_block = MagicMock(spec=ToolUseBlock)
        msg = MagicMock(spec=AssistantMessage)
        msg.content = [tool_block]
        await handle_response_message(msg, api)
        assert not route.called


class TestListenSSE:
    @respx.mock
    @pytest.mark.asyncio
    async def test_non_200_status_uses_http_error_backoff(self, api, interactions, monkeypatch):
        respx.get("https://test.example.com/notifications/stream").mock(
            return_value=httpx.Response(401)
        )
        async def fake_get_session():
            return {}

        monkeypatch.setattr(api, "get_session", fake_get_session)
        sleep_calls = []

        async def fake_sleep(delay):
            sleep_calls.append(delay)
            raise asyncio.CancelledError()

        monkeypatch.setattr(aichat_agent.asyncio, "sleep", fake_sleep)
        with pytest.raises(asyncio.CancelledError):
            await listen_sse(asyncio.Queue(), interactions, api)
        assert sleep_calls == [5]


class TestIPCListener:
    @pytest.mark.asyncio
    async def test_listen_ipc_raises_on_disconnect(self, interactions):
        class FakeIPCClient:
            def __init__(self):
                self._listen_task = asyncio.get_running_loop().create_future()
                self.handler = None

            def on_event(self, handler):
                self.handler = handler

        fake_ipc = FakeIPCClient()
        fake_ipc._listen_task.set_result(None)
        with pytest.raises(ConnectionError, match="IPC listener stopped"):
            await listen_ipc(asyncio.Queue(), interactions, fake_ipc)
        assert fake_ipc.handler is not None

    @pytest.mark.asyncio
    async def test_queue_wait_raises_if_event_listener_exits(self):
        queue: asyncio.Queue[dict] = asyncio.Queue()
        event_task = asyncio.create_task(asyncio.sleep(0))
        await event_task
        with pytest.raises(RuntimeError, match="Event listener task exited unexpectedly"):
            await _await_next_message_or_listener_failure(queue, event_task)

    @pytest.mark.asyncio
    async def test_requires_channel_id_when_ipc_socket_provided(self):
        with pytest.raises(ValueError, match="--channel-id is required"):
            await run_agent(ipc_socket="/tmp/aichat-test.sock")


class TestStartupStatus:
    @pytest.mark.asyncio
    async def test_send_startup_status_is_guarded(self):
        class FakeAPI:
            async def send_tool_status(self, *_args, **_kwargs):
                raise RuntimeError("transient failure")

        hook_state = {"last_send_time": 0}
        await _send_startup_status(FakeAPI(), hook_state)
        assert hook_state["last_send_time"] == 0
