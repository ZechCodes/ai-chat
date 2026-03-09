import base64
import json

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


@pytest.fixture
def ed25519_keypair():
    """Generate a fresh Ed25519 keypair for testing."""
    private_key = Ed25519PrivateKey.generate()
    private_bytes = private_key.private_bytes_raw()
    public_bytes = private_key.public_key().public_bytes_raw()
    return {
        "private_key": private_key,
        "private_b64": base64.b64encode(private_bytes).decode(),
        "public_b64": base64.b64encode(public_bytes).decode(),
    }


@pytest.fixture
def compound_token(ed25519_keypair):
    """Create a compound token for testing."""
    payload = {
        "k": ed25519_keypair["private_b64"],
        "c": "test-channel-123",
        "s": "fake-hmac-signature",
    }
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")


@pytest.fixture
def env_with_token(compound_token, monkeypatch):
    """Set up environment with compound token."""
    monkeypatch.setenv("AICHAT_PRIVATE_KEY", compound_token)
    monkeypatch.setenv("AICHAT_URL", "https://test.example.com")
    return compound_token
