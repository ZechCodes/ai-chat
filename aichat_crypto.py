"""E2E encryption for AI.CHAT — X25519 key exchange + NaCl secretbox.

Single device-level key derived via X25519 ECDH between device and browser.
All message content is encrypted with this key using XSalsa20-Poly1305 (secretbox).

Crypto stack:
- X25519 ECDH: via `cryptography` library
- HKDF-SHA256: key derivation from ECDH shared secret
- XSalsa20-Poly1305 (NaCl secretbox): symmetric authenticated encryption via PyNaCl
"""

from __future__ import annotations

import base64

from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes

import nacl.secret
import nacl.utils

# Salt for HKDF derivation
_HKDF_SALT = b"aichat-device-key"
_HKDF_INFO = b"v1"


# ------------------------------------------------------------------
# X25519 key generation
# ------------------------------------------------------------------


def generate_x25519_keypair() -> dict[str, str]:
    """Generate an X25519 keypair for ECDH key exchange.

    Returns dict with 'private_key_b64' and 'public_key_b64' (base64-encoded raw bytes).
    """
    private_key = X25519PrivateKey.generate()
    public_key = private_key.public_key()
    return {
        "private_key_b64": base64.b64encode(
            private_key.private_bytes_raw()
        ).decode(),
        "public_key_b64": base64.b64encode(
            public_key.public_bytes_raw()
        ).decode(),
    }


# ------------------------------------------------------------------
# NaCl secretbox encryption/decryption
# ------------------------------------------------------------------


def encrypt(key_b64: str, plaintext: str) -> tuple[str, str]:
    """Encrypt a plaintext string with NaCl secretbox.

    Args:
        key_b64: base64-encoded 32-byte symmetric key
        plaintext: UTF-8 string to encrypt

    Returns:
        (ciphertext_b64, nonce_b64) tuple, both base64-encoded.
    """
    key = base64.b64decode(key_b64)
    box = nacl.secret.SecretBox(key)
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
    encrypted = box.encrypt(plaintext.encode("utf-8"), nonce)
    # box.encrypt returns nonce + ciphertext; extract just the ciphertext
    ciphertext = encrypted.ciphertext
    return (
        base64.b64encode(ciphertext).decode(),
        base64.b64encode(nonce).decode(),
    )


def decrypt(key_b64: str, ciphertext_b64: str, nonce_b64: str) -> str:
    """Decrypt a NaCl secretbox ciphertext.

    Args:
        key_b64: base64-encoded 32-byte symmetric key
        ciphertext_b64: base64-encoded ciphertext
        nonce_b64: base64-encoded 24-byte nonce

    Returns:
        Decrypted UTF-8 string.
    """
    key = base64.b64decode(key_b64)
    box = nacl.secret.SecretBox(key)
    ciphertext = base64.b64decode(ciphertext_b64)
    nonce = base64.b64decode(nonce_b64)
    plaintext = box.decrypt(ciphertext, nonce)
    return plaintext.decode("utf-8")


# ------------------------------------------------------------------
# Private helpers for ECDH key derivation
# ------------------------------------------------------------------


def _compute_shared_secret(
    our_private_b64: str, their_public_b64: str
) -> bytes:
    """Compute the raw X25519 shared secret from our private key and their public key."""
    private_key = X25519PrivateKey.from_private_bytes(
        base64.b64decode(our_private_b64)
    )
    public_key = X25519PublicKey.from_public_bytes(
        base64.b64decode(their_public_b64)
    )
    return private_key.exchange(public_key)


def _derive_key(shared_secret: bytes) -> bytes:
    """Derive a 256-bit key from the ECDH shared secret using HKDF-SHA256."""
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=nacl.secret.SecretBox.KEY_SIZE,  # 32 bytes
        salt=_HKDF_SALT,
        info=_HKDF_INFO,
    )
    return hkdf.derive(shared_secret)


# ------------------------------------------------------------------
# KeyExchange — single device-level key abstraction
# ------------------------------------------------------------------


class KeyExchange:
    """Manages the X25519 ECDH key exchange and provides encrypt/decrypt.

    Lifecycle:
    1. Created with device's X25519 keypair
    2. Either restore_key() from persisted config, or complete_exchange() with browser's public key
    3. Once ready, encrypt/decrypt work with the derived key
    """

    def __init__(self, x25519_private_b64: str, x25519_public_b64: str):
        self._private_b64 = x25519_private_b64
        self.public_key_b64 = x25519_public_b64
        self._key_b64: str | None = None

    @property
    def is_ready(self) -> bool:
        return self._key_b64 is not None

    @property
    def key_b64(self) -> str | None:
        return self._key_b64

    def restore_key(self, key_b64: str) -> None:
        """Load a previously derived key from persistent storage."""
        if key_b64:
            self._key_b64 = key_b64

    def complete_exchange(self, their_public_b64: str) -> str:
        """Complete the ECDH exchange with the other party's public key.

        Idempotent: same inputs always produce the same key.
        Returns the derived key as base64.
        """
        shared = _compute_shared_secret(self._private_b64, their_public_b64)
        key = _derive_key(shared)
        self._key_b64 = base64.b64encode(key).decode()
        return self._key_b64

    def encrypt(self, plaintext: str) -> tuple[str, str] | None:
        """Encrypt plaintext. Returns (ciphertext_b64, nonce_b64) or None if not ready."""
        if not self._key_b64:
            return None
        return encrypt(self._key_b64, plaintext)

    def decrypt(self, ciphertext_b64: str, nonce_b64: str) -> str | None:
        """Decrypt ciphertext. Returns plaintext or None if not ready."""
        if not self._key_b64:
            return None
        try:
            return decrypt(self._key_b64, ciphertext_b64, nonce_b64)
        except Exception:
            return None
