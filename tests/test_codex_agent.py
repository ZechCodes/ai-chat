"""Tests for codex_agent helper behavior."""

from codex_agent import _build_mcp_config_overrides


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
