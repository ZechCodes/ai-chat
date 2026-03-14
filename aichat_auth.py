"""Shared Ed25519 request signing for aichat API clients.

Token resolution order:
1. Device auth env vars (AICHAT_DEVICE_KEY + AICHAT_DEVICE_ID + AICHAT_CHANNEL_ID)
2. Direct token parameter
3. ~/.config/aichat/tokens.json keyed by working directory
"""

import base64
import json
import os
import time
import binascii
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

TOKENS_PATH = Path.home() / ".config" / "aichat" / "tokens.json"


def _parse_compound_token(token: str) -> tuple[str, str] | None:
    """Try to parse a compound token. Returns (private_key_b64, channel_id) or None."""
    try:
        pad_len = (-len(token)) % 4
        padded = token + ("=" * pad_len)
        raw = base64.b64decode(padded.encode(), altchars=b"-_", validate=True)
        payload = json.loads(raw.decode())
        if not isinstance(payload, dict):
            return None
        key_b64 = payload.get("k")
        channel_id = payload.get("c")
        signature = payload.get("s")
        if (
            isinstance(key_b64, str)
            and isinstance(channel_id, str)
            and isinstance(signature, str)
            and key_b64
            and channel_id
            and signature
        ):
            return key_b64, channel_id
    except (binascii.Error, ValueError, TypeError, json.JSONDecodeError):
        pass
    except Exception:
        pass
    return None


def _lookup_token_for_cwd() -> str | None:
    """Look up compound token from ~/.config/aichat/tokens.json for the current directory."""
    if not TOKENS_PATH.exists():
        return None
    try:
        tokens = json.loads(TOKENS_PATH.read_text())
        cwd = os.getcwd()
        # Try exact match first, then walk up parent dirs including root.
        while True:
            if cwd in tokens:
                return tokens[cwd]
            parent = os.path.dirname(cwd)
            if parent == cwd:
                break
            cwd = parent
        return None
    except Exception:
        return None


def _has_device_env() -> bool:
    """Check if device auth env vars are set."""
    return bool(os.environ.get("AICHAT_DEVICE_KEY") and os.environ.get("AICHAT_CHANNEL_ID"))


def _get_token() -> str | None:
    """Resolve the compound token from tokens.json."""
    return _lookup_token_for_cwd()


def get_private_key(token: str | None = None) -> Ed25519PrivateKey:
    """Load the Ed25519 private key from token or device env vars."""
    if token is not None:
        parsed = _parse_compound_token(token)
        if not parsed:
            raise SystemExit("Error: Invalid token format (expected compound token)")
        key_b64, _ = parsed
        key_bytes = base64.b64decode(key_b64)
        return Ed25519PrivateKey.from_private_bytes(key_bytes)

    # Device auth env vars take priority when token is not explicitly provided
    device_key = os.environ.get("AICHAT_DEVICE_KEY")
    if device_key:
        key_bytes = base64.b64decode(device_key)
        return Ed25519PrivateKey.from_private_bytes(key_bytes)

    resolved_token = _get_token()
    if not resolved_token:
        raise SystemExit("Error: No aichat token found (set AICHAT_DEVICE_KEY or register with aichat-register)")

    parsed = _parse_compound_token(resolved_token)
    if not parsed:
        raise SystemExit("Error: Invalid token format (expected compound token)")

    key_b64, _ = parsed
    key_bytes = base64.b64decode(key_b64)
    return Ed25519PrivateKey.from_private_bytes(key_bytes)


def get_channel_id(token: str | None = None) -> str | None:
    """Extract channel_id from the compound token or device env vars."""
    if token is not None:
        parsed = _parse_compound_token(token)
        if not parsed:
            return None
        return parsed[1]

    # Device auth env vars take priority when token is not explicitly provided
    channel_id = os.environ.get("AICHAT_CHANNEL_ID")
    if channel_id:
        return channel_id

    resolved_token = _get_token()
    if not resolved_token:
        return None
    parsed = _parse_compound_token(resolved_token)
    if parsed:
        return parsed[1]
    return None


def get_device_id() -> str | None:
    """Get device ID from env var."""
    return os.environ.get("AICHAT_DEVICE_ID")


def has_token() -> bool:
    """Check if auth credentials are available (for hooks to decide whether to no-op)."""
    return _has_device_env() or _get_token() is not None


def sign_request(
    method: str,
    path: str,
    *,
    private_key: Ed25519PrivateKey,
    channel_id: str | None = None,
    device_id: str | None = None,
) -> dict[str, str]:
    """Sign a request and return headers to include.

    Signs: timestamp + method + path
    Returns dict with X-Signature, X-Timestamp, and optionally X-Channel headers.
    """
    timestamp = str(int(time.time()))
    message = f"{timestamp}.{method.upper()}.{path}".encode()
    signature = private_key.sign(message)
    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": base64.b64encode(signature).decode(),
    }
    if channel_id is None:
        channel_id = get_channel_id()
    if channel_id:
        headers["X-Channel"] = channel_id
    if device_id is None:
        device_id = get_device_id()
    if device_id:
        headers["X-Device-Id"] = device_id
    return headers
