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
import binascii

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


def _b64decode_strict(value: str, *, field: str) -> bytes:
    """Strict base64 decode with clear error context."""
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError, TypeError) as e:
        raise ValueError(f"Invalid base64 for {field}") from e


def _decode_secretbox_key(key_b64: str) -> bytes:
    key = _b64decode_strict(key_b64, field="key")
    if len(key) != nacl.secret.SecretBox.KEY_SIZE:
        raise ValueError(
            f"Invalid key length: expected {nacl.secret.SecretBox.KEY_SIZE} bytes, got {len(key)}"
        )
    return key


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
    key = _decode_secretbox_key(key_b64)
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
    key = _decode_secretbox_key(key_b64)
    box = nacl.secret.SecretBox(key)
    ciphertext = _b64decode_strict(ciphertext_b64, field="ciphertext")
    nonce = _b64decode_strict(nonce_b64, field="nonce")
    if len(nonce) != nacl.secret.SecretBox.NONCE_SIZE:
        raise ValueError(
            f"Invalid nonce length: expected {nacl.secret.SecretBox.NONCE_SIZE} bytes, got {len(nonce)}"
        )
    if len(ciphertext) < nacl.secret.SecretBox.MACBYTES:
        raise ValueError(
            f"Invalid ciphertext length: expected at least {nacl.secret.SecretBox.MACBYTES} bytes, got {len(ciphertext)}"
        )
    plaintext = box.decrypt(ciphertext, nonce)
    return plaintext.decode("utf-8")


# ------------------------------------------------------------------
# Private helpers for ECDH key derivation
# ------------------------------------------------------------------


def _compute_shared_secret(
    our_private_b64: str, their_public_b64: str
) -> bytes:
    """Compute the raw X25519 shared secret from our private key and their public key."""
    private_key_raw = _b64decode_strict(our_private_b64, field="x25519 private key")
    public_key_raw = _b64decode_strict(their_public_b64, field="x25519 public key")
    if len(private_key_raw) != 32:
        raise ValueError(f"Invalid x25519 private key length: expected 32 bytes, got {len(private_key_raw)}")
    if len(public_key_raw) != 32:
        raise ValueError(f"Invalid x25519 public key length: expected 32 bytes, got {len(public_key_raw)}")

    private_key = X25519PrivateKey.from_private_bytes(
        private_key_raw
    )
    public_key = X25519PublicKey.from_public_bytes(
        public_key_raw
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
            # Validate persisted key before marking exchange ready.
            key = _decode_secretbox_key(key_b64)
            self._key_b64 = base64.b64encode(key).decode()

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
        """Decrypt ciphertext.

        Returns plaintext, or None if not ready. Raises if ciphertext/nonce
        is invalid or authentication fails.
        """
        if not self._key_b64:
            return None
        return decrypt(self._key_b64, ciphertext_b64, nonce_b64)
