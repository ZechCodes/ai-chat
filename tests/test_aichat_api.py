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
        monkeypatch.delenv("AICHAT_DEVICE_KEY", raising=False)
        monkeypatch.delenv("AICHAT_DEVICE_ID", raising=False)
        monkeypatch.delenv("AICHAT_CHANNEL_ID", raising=False)
        # Point tokens.json lookup to a nonexistent path
        import aichat_auth
        monkeypatch.setattr(aichat_auth, "TOKENS_PATH", tmp_path / "nonexistent.json")
        with pytest.raises(SystemExit):
            AiChatAPI()

    def test_device_key_auth(self, ed25519_keypair, monkeypatch, tmp_path):
        import aichat_auth
        monkeypatch.setattr(aichat_auth, "TOKENS_PATH", tmp_path / "nonexistent.json")
        api = AiChatAPI(
            device_key=ed25519_keypair["private_b64"],
            device_id="dev-test",
            channel_id="ch-test",
        )
        assert api.channel_id == "ch-test"
        assert api.device_id == "dev-test"
        headers = api._sign_request("GET", "/api/messages")
        assert headers["X-Device-Id"] == "dev-test"
        assert headers["X-Channel"] == "ch-test"


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


class TestMarkRead:
    @respx.mock
    @pytest.mark.asyncio
    async def test_marks_messages_read(self, api):
        route = respx.post("https://test.example.com/api/messages/read").mock(
            return_value=httpx.Response(200, json={"marked": ["msg-1", "msg-2"]})
        )
        result = await api.mark_read(["msg-1", "msg-2"])
        assert result == {"marked": ["msg-1", "msg-2"]}
        body = json.loads(route.calls[0].request.content)
        assert body["message_ids"] == ["msg-1", "msg-2"]

    @pytest.mark.asyncio
    async def test_skips_empty_list(self, api):
        result = await api.mark_read([])
        assert result == {"marked": []}

    @respx.mock
    @pytest.mark.asyncio
    async def test_includes_auth_headers(self, api):
        route = respx.post("https://test.example.com/api/messages/read").mock(
            return_value=httpx.Response(200, json={"marked": ["msg-1"]})
        )
        await api.mark_read(["msg-1"])
        headers = route.calls[0].request.headers
        assert "x-timestamp" in headers
        assert "x-signature" in headers
        assert "x-channel" in headers


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
