"""Tests for the unified async API client."""

import json

import httpx
import pytest
import respx

from aichat_api import AiChatAPI


@pytest.fixture
def api(env_with_token):
    """Create an AiChatAPI instance with test token."""
    return AiChatAPI(token=env_with_token)


class TestAiChatAPIInit:
    def test_resolves_base_url_from_env(self, api):
        assert api.base_url == "https://test.example.com"

    def test_resolves_channel_id_from_token(self, api):
        assert api.channel_id == "test-channel-123"

    def test_has_private_key(self, api):
        assert api._private_key is not None

    def test_raises_without_token(self, monkeypatch, tmp_path):
        monkeypatch.delenv("AICHAT_PRIVATE_KEY", raising=False)
        # Point tokens.json lookup to a nonexistent path
        import aichat_auth
        monkeypatch.setattr(aichat_auth, "TOKENS_PATH", tmp_path / "nonexistent.json")
        with pytest.raises(SystemExit):
            AiChatAPI()


class TestSignRequest:
    def test_returns_required_headers(self, api):
        headers = api._sign_request("GET", "/api/messages")
        assert "X-Timestamp" in headers
        assert "X-Signature" in headers
        assert "X-Channel" in headers
        assert headers["X-Channel"] == "test-channel-123"

    def test_signature_is_valid_base64(self, api):
        import base64

        headers = api._sign_request("POST", "/api/messages")
        # Should not raise
        base64.b64decode(headers["X-Signature"])


class TestSendMessage:
    @respx.mock
    @pytest.mark.asyncio
    async def test_sends_message(self, api):
        route = respx.post("https://test.example.com/api/messages").mock(
            return_value=httpx.Response(200, json={"ok": True, "id": "msg-1"})
        )
        result = await api.send_message("hello")
        assert result == {"ok": True, "id": "msg-1"}
        assert route.called
        body = json.loads(route.calls[0].request.content)
        assert body["content"] == "hello"

    @respx.mock
    @pytest.mark.asyncio
    async def test_includes_auth_headers(self, api):
        route = respx.post("https://test.example.com/api/messages").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        await api.send_message("test")
        headers = route.calls[0].request.headers
        assert "x-timestamp" in headers
        assert "x-signature" in headers
        assert "x-channel" in headers


class TestGetUnread:
    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_messages(self, api):
        msgs = [{"content": "hey", "created_at": "2026-03-09T00:00:00Z"}]
        respx.get("https://test.example.com/api/messages/unread").mock(
            return_value=httpx.Response(200, json=msgs)
        )
        result = await api.get_unread()
        assert result == msgs

    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_empty_list(self, api):
        respx.get("https://test.example.com/api/messages/unread").mock(
            return_value=httpx.Response(200, json=[])
        )
        result = await api.get_unread()
        assert result == []


class TestGetMessages:
    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_messages_with_default_limit(self, api):
        msgs = [{"content": "msg1"}, {"content": "msg2"}]
        route = respx.get("https://test.example.com/api/messages").mock(
            return_value=httpx.Response(200, json=msgs)
        )
        result = await api.get_messages()
        assert result == msgs

    @respx.mock
    @pytest.mark.asyncio
    async def test_passes_limit_param(self, api):
        route = respx.get("https://test.example.com/api/messages").mock(
            return_value=httpx.Response(200, json=[])
        )
        await api.get_messages(limit=5)
        assert "limit=5" in str(route.calls[0].request.url)

    @respx.mock
    @pytest.mark.asyncio
    async def test_passes_before_param(self, api):
        route = respx.get("https://test.example.com/api/messages").mock(
            return_value=httpx.Response(200, json=[])
        )
        await api.get_messages(before="msg-abc")
        assert "before=msg-abc" in str(route.calls[0].request.url)


class TestSendToolStatus:
    @respx.mock
    @pytest.mark.asyncio
    async def test_sends_active_status(self, api):
        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        await api.send_tool_status("active", tool="Read", description="Reading file")
        body = json.loads(route.calls[0].request.content)
        assert body["status"] == "active"
        assert body["tool"] == "Read"
        assert body["description"] == "Reading file"

    @respx.mock
    @pytest.mark.asyncio
    async def test_sends_idle_status(self, api):
        route = respx.post("https://test.example.com/api/tool-status").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        await api.send_tool_status("idle")
        body = json.loads(route.calls[0].request.content)
        assert body["status"] == "idle"


class TestGetSession:
    @respx.mock
    @pytest.mark.asyncio
    async def test_gets_session_cookies(self, api):
        respx.post("https://test.example.com/api/session").mock(
            return_value=httpx.Response(
                200,
                json={"ok": True},
                headers={"Set-Cookie": "session=abc123; Path=/"},
            )
        )
        cookies = await api.get_session()
        assert cookies is not None
