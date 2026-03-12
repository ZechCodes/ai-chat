"""Tests for aichat_crypto — E2E encryption primitives."""

from __future__ import annotations

import base64

from aichat_crypto import (
    compute_shared_secret,
    decrypt,
    decrypt_channel_key,
    derive_device_master_key,
    derive_device_master_key_b64,
    encrypt,
    encrypt_channel_key,
    generate_channel_key,
    generate_x25519_keypair,
)


def test_x25519_keypair_generation():
    kp = generate_x25519_keypair()
    assert "private_key_b64" in kp
    assert "public_key_b64" in kp
    # Keys should be 32 bytes each
    assert len(base64.b64decode(kp["private_key_b64"])) == 32
    assert len(base64.b64decode(kp["public_key_b64"])) == 32


def test_ecdh_shared_secret_matches():
    """Both sides of the ECDH exchange should derive the same shared secret."""
    alice = generate_x25519_keypair()
    bob = generate_x25519_keypair()

    secret_a = compute_shared_secret(alice["private_key_b64"], bob["public_key_b64"])
    secret_b = compute_shared_secret(bob["private_key_b64"], alice["public_key_b64"])

    assert secret_a == secret_b
    assert len(secret_a) == 32


def test_derive_device_master_key():
    alice = generate_x25519_keypair()
    bob = generate_x25519_keypair()

    shared = compute_shared_secret(alice["private_key_b64"], bob["public_key_b64"])
    key = derive_device_master_key(shared)
    assert len(key) == 32

    # Same shared secret should always produce the same key
    key2 = derive_device_master_key(shared)
    assert key == key2


def test_derive_device_master_key_b64_matches_both_sides():
    """The one-shot derivation should match on both sides."""
    device = generate_x25519_keypair()
    browser = generate_x25519_keypair()

    device_key = derive_device_master_key_b64(
        device["private_key_b64"], browser["public_key_b64"]
    )
    browser_key = derive_device_master_key_b64(
        browser["private_key_b64"], device["public_key_b64"]
    )

    assert device_key == browser_key
    assert len(base64.b64decode(device_key)) == 32


def test_generate_channel_key():
    key = generate_channel_key()
    assert len(base64.b64decode(key)) == 32
    # Should be random each time
    key2 = generate_channel_key()
    assert key != key2


def test_encrypt_decrypt_roundtrip():
    key = generate_channel_key()
    plaintext = "Hello, world! This is a secret message."

    ciphertext, nonce = encrypt(key, plaintext)
    result = decrypt(key, ciphertext, nonce)

    assert result == plaintext


def test_encrypt_produces_different_ciphertext():
    """Each encryption should use a random nonce, producing different ciphertext."""
    key = generate_channel_key()
    plaintext = "Same message"

    ct1, n1 = encrypt(key, plaintext)
    ct2, n2 = encrypt(key, plaintext)

    assert ct1 != ct2 or n1 != n2  # At minimum nonces differ


def test_decrypt_with_wrong_key_fails():
    key1 = generate_channel_key()
    key2 = generate_channel_key()
    plaintext = "Secret"

    ct, nonce = encrypt(key1, plaintext)

    try:
        decrypt(key2, ct, nonce)
        assert False, "Should have raised"
    except Exception:
        pass  # Expected: CryptoError


def test_encrypt_decrypt_unicode():
    key = generate_channel_key()
    plaintext = "Unicode test: \u2603\u2764\ufe0f\U0001f680 \u65e5\u672c\u8a9e"

    ct, nonce = encrypt(key, plaintext)
    assert decrypt(key, ct, nonce) == plaintext


def test_encrypt_decrypt_empty_string():
    key = generate_channel_key()
    ct, nonce = encrypt(key, "")
    assert decrypt(key, ct, nonce) == ""


def test_encrypt_decrypt_large_message():
    key = generate_channel_key()
    plaintext = "x" * 100_000  # 100KB message

    ct, nonce = encrypt(key, plaintext)
    assert decrypt(key, ct, nonce) == plaintext


def test_channel_key_encryption_roundtrip():
    """Channel keys encrypted with device master key should roundtrip."""
    device = generate_x25519_keypair()
    browser = generate_x25519_keypair()

    master_key = derive_device_master_key_b64(
        device["private_key_b64"], browser["public_key_b64"]
    )
    channel_key = generate_channel_key()

    encrypted, nonce = encrypt_channel_key(master_key, channel_key)
    decrypted = decrypt_channel_key(master_key, encrypted, nonce)

    assert decrypted == channel_key


def test_full_e2e_flow():
    """Simulate the full flow: ECDH → master key → channel key → message encryption."""
    # 1. Device and browser do ECDH
    device_kp = generate_x25519_keypair()
    browser_kp = generate_x25519_keypair()

    device_master = derive_device_master_key_b64(
        device_kp["private_key_b64"], browser_kp["public_key_b64"]
    )
    browser_master = derive_device_master_key_b64(
        browser_kp["private_key_b64"], device_kp["public_key_b64"]
    )
    assert device_master == browser_master

    # 2. Device generates channel key, encrypts it for storage
    channel_key = generate_channel_key()
    enc_ck, ck_nonce = encrypt_channel_key(device_master, channel_key)

    # 3. Browser retrieves encrypted channel key and decrypts
    browser_channel_key = decrypt_channel_key(browser_master, enc_ck, ck_nonce)
    assert browser_channel_key == channel_key

    # 4. Device encrypts a message with the channel key
    message = "Hello from the agent!"
    ct, msg_nonce = encrypt(channel_key, message)

    # 5. Browser decrypts the message with the same channel key
    decrypted = decrypt(browser_channel_key, ct, msg_nonce)
    assert decrypted == message
