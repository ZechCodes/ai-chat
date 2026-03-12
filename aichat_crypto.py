"""E2E encryption for AI.CHAT — X25519 key exchange + NaCl secretbox.

Key concepts:
- Device master key: derived via X25519 ECDH between device and browser during
  device approval. Used to encrypt per-channel symmetric keys.
- Channel key: random 256-bit key for XSalsa20-Poly1305 (secretbox). Each channel
  has its own key. All message content is encrypted with the channel key.

Crypto stack:
- X25519 ECDH: via `cryptography` library (already a dependency)
- HKDF-SHA256: key derivation from ECDH shared secret
- XSalsa20-Poly1305 (NaCl secretbox): symmetric authenticated encryption via PyNaCl
"""

from __future__ import annotations

import base64
import os

from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes

import nacl.secret
import nacl.utils

# Salt for HKDF derivation of device master key
_HKDF_SALT = b"aichat-device-key"
_HKDF_INFO = b"v1"


# ------------------------------------------------------------------
# X25519 key generation and ECDH
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


def compute_shared_secret(
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


def derive_device_master_key(shared_secret: bytes) -> bytes:
    """Derive a 256-bit device master key from the ECDH shared secret using HKDF-SHA256."""
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=nacl.secret.SecretBox.KEY_SIZE,  # 32 bytes
        salt=_HKDF_SALT,
        info=_HKDF_INFO,
    )
    return hkdf.derive(shared_secret)


def derive_device_master_key_b64(
    our_private_b64: str, their_public_b64: str
) -> str:
    """One-shot: compute ECDH shared secret and derive device master key.

    Returns base64-encoded 32-byte key.
    """
    shared = compute_shared_secret(our_private_b64, their_public_b64)
    key = derive_device_master_key(shared)
    return base64.b64encode(key).decode()


# ------------------------------------------------------------------
# Channel key generation
# ------------------------------------------------------------------


def generate_channel_key() -> str:
    """Generate a random 256-bit channel encryption key.

    Returns base64-encoded 32-byte key.
    """
    key = os.urandom(nacl.secret.SecretBox.KEY_SIZE)
    return base64.b64encode(key).decode()


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
# Convenience: encrypt/decrypt channel keys with device master key
# ------------------------------------------------------------------


def encrypt_channel_key(
    device_master_key_b64: str, channel_key_b64: str
) -> tuple[str, str]:
    """Encrypt a channel key with the device master key.

    Returns (encrypted_key_b64, nonce_b64).
    """
    return encrypt(device_master_key_b64, channel_key_b64)


def decrypt_channel_key(
    device_master_key_b64: str,
    encrypted_key_b64: str,
    nonce_b64: str,
) -> str:
    """Decrypt a channel key using the device master key.

    Returns the plaintext channel_key_b64 string.
    """
    return decrypt(device_master_key_b64, encrypted_key_b64, nonce_b64)
