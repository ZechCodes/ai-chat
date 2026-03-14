"""Tests for aichat_crypto — E2E encryption primitives and KeyExchange."""

from __future__ import annotations

import base64
import os

import nacl.exceptions
import pytest

from aichat_crypto import (
    KeyExchange,
    decrypt,
    encrypt,
    generate_x25519_keypair,
)


def test_x25519_keypair_generation():
    kp = generate_x25519_keypair()
    assert "private_key_b64" in kp
    assert "public_key_b64" in kp
    # Keys should be 32 bytes each
    assert len(base64.b64decode(kp["private_key_b64"])) == 32
    assert len(base64.b64decode(kp["public_key_b64"])) == 32


def test_encrypt_decrypt_roundtrip():
    key = base64.b64encode(os.urandom(32)).decode()
    plaintext = "Hello, world! This is a secret message."

    ciphertext, nonce = encrypt(key, plaintext)
    result = decrypt(key, ciphertext, nonce)

    assert result == plaintext


def test_encrypt_produces_different_ciphertext():
    """Each encryption should use a random nonce, producing different ciphertext."""
    key = base64.b64encode(os.urandom(32)).decode()
    plaintext = "Same message"

    ct1, n1 = encrypt(key, plaintext)
    ct2, n2 = encrypt(key, plaintext)

    assert ct1 != ct2 or n1 != n2  # At minimum nonces differ


def test_decrypt_with_wrong_key_fails():
    key1 = base64.b64encode(os.urandom(32)).decode()
    key2 = base64.b64encode(os.urandom(32)).decode()
    plaintext = "Secret"

    ct, nonce = encrypt(key1, plaintext)

    with pytest.raises(nacl.exceptions.CryptoError):
        decrypt(key2, ct, nonce)


def test_encrypt_decrypt_unicode():
    key = base64.b64encode(os.urandom(32)).decode()
    plaintext = "Unicode test: \u2603\u2764\ufe0f\U0001f680 \u65e5\u672c\u8a9e"

    ct, nonce = encrypt(key, plaintext)
    assert decrypt(key, ct, nonce) == plaintext


def test_encrypt_decrypt_empty_string():
    key = base64.b64encode(os.urandom(32)).decode()
    ct, nonce = encrypt(key, "")
    assert decrypt(key, ct, nonce) == ""


def test_encrypt_decrypt_large_message():
    key = base64.b64encode(os.urandom(32)).decode()
    plaintext = "x" * 100_000  # 100KB message

    ct, nonce = encrypt(key, plaintext)
    assert decrypt(key, ct, nonce) == plaintext


# ------------------------------------------------------------------
# KeyExchange tests
# ------------------------------------------------------------------


def test_key_exchange_lifecycle():
    """Full lifecycle: generate keypairs, exchange, encrypt, decrypt."""
    device = generate_x25519_keypair()
    browser = generate_x25519_keypair()

    device_kx = KeyExchange(device["private_key_b64"], device["public_key_b64"])
    browser_kx = KeyExchange(browser["private_key_b64"], browser["public_key_b64"])

    assert not device_kx.is_ready
    assert not browser_kx.is_ready

    # Complete exchange on both sides
    device_key = device_kx.complete_exchange(browser["public_key_b64"])
    browser_key = browser_kx.complete_exchange(device["public_key_b64"])

    assert device_kx.is_ready
    assert browser_kx.is_ready
    assert device_key == browser_key
    assert len(base64.b64decode(device_key)) == 32

    # Device encrypts, browser decrypts
    message = "Hello from the agent!"
    encrypted = device_kx.encrypt(message)
    assert encrypted is not None
    ct, nonce = encrypted
    decrypted = browser_kx.decrypt(ct, nonce)
    assert decrypted == message


def test_rekey_idempotent():
    """Same inputs to complete_exchange always produce the same key."""
    device = generate_x25519_keypair()
    browser = generate_x25519_keypair()

    kx = KeyExchange(device["private_key_b64"], device["public_key_b64"])
    key1 = kx.complete_exchange(browser["public_key_b64"])
    key2 = kx.complete_exchange(browser["public_key_b64"])

    assert key1 == key2


def test_encrypt_before_exchange_returns_none():
    """Encrypt/decrypt should return None before key exchange is completed."""
    kp = generate_x25519_keypair()
    kx = KeyExchange(kp["private_key_b64"], kp["public_key_b64"])

    assert kx.encrypt("hello") is None
    assert kx.decrypt("abc", "def") is None


def test_restore_key():
    """restore_key should make the exchange ready without ECDH."""
    device = generate_x25519_keypair()
    browser = generate_x25519_keypair()

    # First: do a real exchange to get a key
    kx1 = KeyExchange(device["private_key_b64"], device["public_key_b64"])
    key = kx1.complete_exchange(browser["public_key_b64"])

    # Second: restore from persisted key
    kx2 = KeyExchange(device["private_key_b64"], device["public_key_b64"])
    assert not kx2.is_ready
    kx2.restore_key(key)
    assert kx2.is_ready
    assert kx2.key_b64 == key

    # Should be able to encrypt/decrypt
    encrypted = kx2.encrypt("test message")
    assert encrypted is not None
    ct, nonce = encrypted
    assert kx1.decrypt(ct, nonce) == "test message"


def test_restore_key_rejects_invalid_key():
    kp = generate_x25519_keypair()
    kx = KeyExchange(kp["private_key_b64"], kp["public_key_b64"])

    with pytest.raises(ValueError):
        kx.restore_key("not-base64")
    assert not kx.is_ready


def test_encrypt_rejects_non_strict_base64_key():
    key_with_junk = "YWJj***"  # decodes in permissive mode, should fail in strict mode
    with pytest.raises(ValueError):
        encrypt(key_with_junk, "hello")


def test_decrypt_rejects_invalid_nonce_length():
    key = base64.b64encode(os.urandom(32)).decode()
    ct, _ = encrypt(key, "hello")
    short_nonce = base64.b64encode(b"short").decode()
    with pytest.raises(ValueError):
        decrypt(key, ct, short_nonce)


def test_keyexchange_decrypt_propagates_auth_failure():
    device = generate_x25519_keypair()
    browser = generate_x25519_keypair()
    device_kx = KeyExchange(device["private_key_b64"], device["public_key_b64"])
    browser_kx = KeyExchange(browser["private_key_b64"], browser["public_key_b64"])

    device_kx.complete_exchange(browser["public_key_b64"])
    browser_kx.complete_exchange(device["public_key_b64"])

    encrypted = device_kx.encrypt("hello")
    assert encrypted is not None
    ct, nonce = encrypted
    tampered_ct = ct[:-4] + ("AAAA" if len(ct) >= 4 else ct)

    with pytest.raises((ValueError, nacl.exceptions.CryptoError)):
        browser_kx.decrypt(tampered_ct, nonce)


def test_full_e2e_flow():
    """Simulate the full flow: ECDH → encrypt message, no channel key layer."""
    # 1. Device and browser do ECDH
    device_kp = generate_x25519_keypair()
    browser_kp = generate_x25519_keypair()

    device_kx = KeyExchange(device_kp["private_key_b64"], device_kp["public_key_b64"])
    browser_kx = KeyExchange(browser_kp["private_key_b64"], browser_kp["public_key_b64"])

    device_kx.complete_exchange(browser_kp["public_key_b64"])
    browser_kx.complete_exchange(device_kp["public_key_b64"])

    assert device_kx.key_b64 == browser_kx.key_b64

    # 2. Device encrypts a message
    message = "Hello from the agent!"
    encrypted = device_kx.encrypt(message)
    assert encrypted is not None
    ct, nonce = encrypted

    # 3. Browser decrypts the message
    decrypted = browser_kx.decrypt(ct, nonce)
    assert decrypted == message
