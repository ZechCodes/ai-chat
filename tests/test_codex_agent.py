"""Tests for codex_agent helper behavior."""

import asyncio

import pytest

from codex_agent import (
    SSE_CONTEXT,
    _await_next_message_or_listener_failure,
    _build_mcp_config_overrides,
    _format_mcp_tool_name,
    run_agent,
)


def test_build_mcp_config_overrides_with_ipc_and_auth():
    overrides = _build_mcp_config_overrides(
        "ch-123",
        ipc_socket="/tmp/aichat.sock",
        base_url="https://aichat.zech.sh",
        token="tok-abc",
        device_key="dev-key",
        device_id="dev-1",
    )

    assert 'mcp_servers.aichat.type="stdio"' in overrides
    assert 'mcp_servers.aichat.command="uv"' in overrides
    assert any("aichat_mcp_server.py" in item for item in overrides)
    assert 'mcp_servers.aichat.env.AICHAT_CHANNEL_ID="ch-123"' in overrides
    assert 'mcp_servers.aichat.env.AICHAT_IPC_SOCKET="/tmp/aichat.sock"' in overrides
    assert 'mcp_servers.aichat.env.AICHAT_BASE_URL="https://aichat.zech.sh"' in overrides
    assert 'mcp_servers.aichat.env.AICHAT_TOKEN="tok-abc"' in overrides
    assert 'mcp_servers.aichat.env.AICHAT_DEVICE_KEY="dev-key"' in overrides
    assert 'mcp_servers.aichat.env.AICHAT_DEVICE_ID="dev-1"' in overrides


def test_build_mcp_config_overrides_direct_mode_omits_ipc():
    overrides = _build_mcp_config_overrides(
        "ch-xyz",
        token="tok-direct",
    )

    assert 'mcp_servers.aichat.env.AICHAT_CHANNEL_ID="ch-xyz"' in overrides
    assert 'mcp_servers.aichat.env.AICHAT_TOKEN="tok-direct"' in overrides
    assert all("AICHAT_IPC_SOCKET" not in item for item in overrides)


def test_sse_context_includes_planning_guidance():
    assert "For complex tasks, use planning mode" in SSE_CONTEXT
    assert "AskUserQuestion tool" in SSE_CONTEXT


def test_format_mcp_tool_name_uses_double_underscore_form():
    assert _format_mcp_tool_name("aichat", "send") == "mcp__aichat__send"
    assert _format_mcp_tool_name("", "send") == "mcp__send"
    assert _format_mcp_tool_name("aichat", "") == "mcp__aichat"


@pytest.mark.asyncio
async def test_queue_wait_raises_if_event_listener_exits():
    queue: asyncio.Queue[dict] = asyncio.Queue()
    event_task = asyncio.create_task(asyncio.sleep(0))
    await event_task
    with pytest.raises(RuntimeError, match="Event listener task exited unexpectedly"):
        await _await_next_message_or_listener_failure(queue, event_task)


@pytest.mark.asyncio
async def test_requires_channel_id_when_ipc_socket_provided():
    with pytest.raises(ValueError, match="--channel-id is required"):
        await run_agent(ipc_socket="/tmp/aichat-test.sock")
