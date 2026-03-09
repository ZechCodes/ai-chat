"""Tests for device auth flow."""

import base64
import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
import respx

from aichat_device_auth import (
    DeviceConfig,
    generate_device_keypair,
    load_device_config,
    save_device_config,
    register_device,
    poll_for_approval,
)


class TestGenerateDeviceKeypair:
    def test_returns_private_and_public_keys(self):
        keypair = generate_device_keypair()
        assert "private_key" in keypair
        assert "public_key_b64" in keypair
        assert "private_key_b64" in keypair

    def test_public_key_is_base64(self):
        keypair = generate_device_keypair()
        # Should decode without error
        decoded = base64.b64decode(keypair["public_key_b64"])
        assert len(decoded) == 32  # Ed25519 public key is 32 bytes

    def test_private_key_is_base64(self):
        keypair = generate_device_keypair()
        decoded = base64.b64decode(keypair["private_key_b64"])
        assert len(decoded) == 32  # Ed25519 private key raw is 32 bytes

    def test_generates_unique_keys(self):
        kp1 = generate_device_keypair()
        kp2 = generate_device_keypair()
        assert kp1["public_key_b64"] != kp2["public_key_b64"]


class TestDeviceConfig:
    def test_save_and_load(self, tmp_path):
        config_path = tmp_path / "device.json"
        config = DeviceConfig(
            device_id="dev-123",
            device_name="Test Device",
            private_key_b64="abc123",
            public_key_b64="def456",
            base_url="https://aichat.zech.sh",
        )
        save_device_config(config, config_path)
        loaded = load_device_config(config_path)
        assert loaded is not None
        assert loaded.device_id == "dev-123"
        assert loaded.device_name == "Test Device"
        assert loaded.private_key_b64 == "abc123"
        assert loaded.public_key_b64 == "def456"
        assert loaded.base_url == "https://aichat.zech.sh"

    def test_load_nonexistent_returns_none(self, tmp_path):
        config_path = tmp_path / "nonexistent.json"
        assert load_device_config(config_path) is None

    def test_load_corrupt_returns_none(self, tmp_path):
        config_path = tmp_path / "device.json"
        config_path.write_text("not valid json{{{")
        assert load_device_config(config_path) is None

    def test_save_creates_parent_dirs(self, tmp_path):
        config_path = tmp_path / "nested" / "dir" / "device.json"
        config = DeviceConfig(
            device_id="dev-1",
            device_name="Test",
            private_key_b64="k",
            public_key_b64="p",
            base_url="https://example.com",
        )
        save_device_config(config, config_path)
        assert config_path.exists()

    def test_saved_file_has_restricted_permissions(self, tmp_path):
        config_path = tmp_path / "device.json"
        config = DeviceConfig(
            device_id="dev-1",
            device_name="Test",
            private_key_b64="k",
            public_key_b64="p",
            base_url="https://example.com",
        )
        save_device_config(config, config_path)
        mode = config_path.stat().st_mode & 0o777
        assert mode == 0o600


class TestRegisterDevice:
    @respx.mock
    @pytest.mark.asyncio
    async def test_successful_registration(self):
        route = respx.post("https://aichat.zech.sh/api/devices/register").mock(
            return_value=httpx.Response(200, json={
                "device_code": "ABCD-1234",
                "auth_url": "https://aichat.zech.sh/devices/authorize?code=ABCD-1234",
            })
        )
        result = await register_device(
            base_url="https://aichat.zech.sh",
            public_key_b64="test-pub-key",
            device_name="My MacBook",
        )
        assert result["device_code"] == "ABCD-1234"
        assert "auth_url" in result
        assert route.called
        body = json.loads(route.calls[0].request.content)
        assert body["public_key"] == "test-pub-key"
        assert body["name"] == "My MacBook"

    @respx.mock
    @pytest.mark.asyncio
    async def test_registration_failure(self):
        respx.post("https://aichat.zech.sh/api/devices/register").mock(
            return_value=httpx.Response(500, json={"error": "server error"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            await register_device(
                base_url="https://aichat.zech.sh",
                public_key_b64="key",
                device_name="Dev",
            )


class TestPollForApproval:
    @respx.mock
    @pytest.mark.asyncio
    async def test_approved(self):
        respx.get("https://aichat.zech.sh/api/devices/status").mock(
            return_value=httpx.Response(200, json={
                "status": "approved",
                "device_id": "dev-abc-123",
            })
        )
        result = await poll_for_approval(
            base_url="https://aichat.zech.sh",
            device_code="ABCD-1234",
        )
        assert result is not None
        assert result["status"] == "approved"
        assert result["device_id"] == "dev-abc-123"

    @respx.mock
    @pytest.mark.asyncio
    async def test_pending(self):
        respx.get("https://aichat.zech.sh/api/devices/status").mock(
            return_value=httpx.Response(200, json={
                "status": "pending",
            })
        )
        result = await poll_for_approval(
            base_url="https://aichat.zech.sh",
            device_code="ABCD-1234",
        )
        assert result is not None
        assert result["status"] == "pending"

    @respx.mock
    @pytest.mark.asyncio
    async def test_denied(self):
        respx.get("https://aichat.zech.sh/api/devices/status").mock(
            return_value=httpx.Response(200, json={
                "status": "denied",
            })
        )
        result = await poll_for_approval(
            base_url="https://aichat.zech.sh",
            device_code="ABCD-1234",
        )
        assert result["status"] == "denied"

    @respx.mock
    @pytest.mark.asyncio
    async def test_expired(self):
        respx.get("https://aichat.zech.sh/api/devices/status").mock(
            return_value=httpx.Response(410, json={
                "error": "device_code expired",
            })
        )
        with pytest.raises(httpx.HTTPStatusError):
            await poll_for_approval(
                base_url="https://aichat.zech.sh",
                device_code="ABCD-1234",
            )
