"""Device-level auth for AI.CHAT manager.

Handles Ed25519 keypair generation, X25519 key exchange for E2E encryption,
device registration, approval polling, and persistent device config storage.
"""

from __future__ import annotations

import base64
import json
import os
import stat
from dataclasses import asdict, dataclass, field
from pathlib import Path

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "aichat" / "device.json"


@dataclass
class DeviceConfig:
    """Persistent device configuration stored locally."""

    device_id: str
    device_name: str
    private_key_b64: str
    public_key_b64: str
    base_url: str
    workers: dict[str, dict] = field(default_factory=dict)
    # X25519 private key for E2E key exchange (persisted for re-keying)
    x25519_private_b64: str = ""
    x25519_public_b64: str = ""
    # Encryption key derived from ECDH (encrypts all message content)
    encryption_key_b64: str = ""


def generate_device_keypair() -> dict:
    """Generate a new Ed25519 keypair for device auth.

    Returns dict with 'private_key' (Ed25519PrivateKey object),
    'private_key_b64' (base64-encoded raw bytes),
    and 'public_key_b64' (base64-encoded raw bytes).
    """
    private_key = Ed25519PrivateKey.generate()
    return {
        "private_key": private_key,
        "private_key_b64": base64.b64encode(private_key.private_bytes_raw()).decode(),
        "public_key_b64": base64.b64encode(private_key.public_key().public_bytes_raw()).decode(),
    }


def save_device_config(config: DeviceConfig, path: Path = DEFAULT_CONFIG_PATH) -> None:
    """Save device config to disk with restricted permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(config), indent=2))
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # 0o600


def load_device_config(path: Path = DEFAULT_CONFIG_PATH) -> DeviceConfig | None:
    """Load device config from disk. Returns None if missing or corrupt."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        data.setdefault("workers", {})
        data.setdefault("x25519_private_b64", "")
        data.setdefault("x25519_public_b64", "")
        # Migrate old field name
        if "device_master_key_b64" in data:
            data.setdefault("encryption_key_b64", data.pop("device_master_key_b64"))
        data.setdefault("encryption_key_b64", "")
        return DeviceConfig(**data)
    except Exception:
        return None


async def register_device(
    base_url: str,
    public_key_b64: str,
    device_name: str,
    x25519_public_b64: str = "",
) -> dict:
    """Register a new device with the server.

    POST /api/devices/register
    Returns {"device_code": "...", "auth_url": "..."}.
    """
    payload = {"public_key": public_key_b64, "name": device_name}
    if x25519_public_b64:
        payload["x25519_public"] = x25519_public_b64
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{base_url}/api/devices/register",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()


async def poll_for_approval(base_url: str, device_code: str) -> dict:
    """Poll the server for device approval status.

    GET /api/devices/status?code={device_code}
    Returns {"status": "pending|approved|denied", ...}.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{base_url}/api/devices/status",
            params={"code": device_code},
        )
        resp.raise_for_status()
        return resp.json()
