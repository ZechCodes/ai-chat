"""Tests for the agent wrapper."""

import json
from unittest.mock import MagicMock

import httpx
import pytest
import respx

from aichat_api import AiChatAPI
from aichat_agent import build_agent_options, handle_response_message
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
