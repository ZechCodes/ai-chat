# E2E Encryption Implementation Guide

A portable blueprint for re-implementing ai-chat's end-to-end encryption in another project. The server acts as a zero-knowledge relay — it never sees plaintext message content.

---

## Architecture Overview

```
┌──────────┐         ┌──────────┐         ┌──────────┐
│ Browser  │◄──SSE──►│  Server  │◄──WS───►│  Device  │
│ (user)   │         │ (relay)  │         │ (agent)  │
└──────────┘         └──────────┘         └──────────┘
     │                    │                    │
     │  E2E encrypted     │  "[encrypted]"     │  plaintext
     │  payloads only     │  stored in DB      │  in local DB
```

**Participants:**
- **Device** — the agent/bot host. Generates and holds the canonical encryption key. Runs workers that process messages in plaintext.
- **Browser** — the user's client. Obtains the encryption key via ECDH key exchange with the device.
- **Server** — relays encrypted payloads between browser and device. Stores `"[encrypted]"` as message content. Never has access to the encryption key or plaintext.

---

## 1. Cryptographic Primitives

| Purpose | Algorithm | Library (Python) | Library (JS) |
|---------|-----------|-------------------|--------------|
| Key exchange | X25519 ECDH | `cryptography` (`X25519PrivateKey`) | `tweetnacl` (`nacl.scalarMult`) |
| Key derivation | HKDF-SHA256 | `cryptography` (`HKDF`) | Web Crypto API (`deriveBits`) |
| Symmetric encryption | XSalsa20-Poly1305 | `PyNaCl` (`nacl.secret.SecretBox`) | `tweetnacl` (`nacl.secretbox`) |
| Request signing (auth) | Ed25519 | `cryptography` (`Ed25519PrivateKey`) | — |

### Parameters

- **SecretBox key**: 32 bytes
- **SecretBox nonce**: 24 bytes (random per encryption)
- **SecretBox MAC**: 16 bytes (appended to ciphertext automatically)
- **X25519 keys**: 32 bytes (private and public)
- **HKDF salt**: `b"aichat-device-key"` (fixed)
- **HKDF info**: `b"v1"` (fixed)
- **HKDF output**: 256 bits (32 bytes)

---

## 2. Key Hierarchy

```
Device
├── Ed25519 keypair ─── request signing (authentication)
├── X25519 keypair ──── ECDH key exchange (transport key derivation)
└── Encryption key ──── random 32 bytes, generated once on first startup
                        All message content encrypted with this key
                        Never rotated (simplicity trade-off)

Browser Session
├── Ephemeral X25519 keypair ─── generated per rekey
└── Transport key ─────────────── derived via ECDH, used once to unwrap encryption key
```

The **encryption key** is the single canonical key used for all message encryption/decryption. It's a random 32-byte value, not derived from ECDH. ECDH is used solely as a transport mechanism to securely deliver this key to browser sessions.

---

## 3. Device Registration & Authorization

### 3.1 Device Setup (one-time)

1. Device generates an **Ed25519 keypair** (for request signing)
2. Device generates an **X25519 keypair** (for ECDH key exchange)
3. Device generates a random **32-byte encryption key**
4. All keys persisted to `~/.config/aichat/device.json` with `0o600` permissions

### 3.2 Registration Flow

```
Device                          Server                          Browser (user)
  │                               │                               │
  ├── POST /api/devices/register ─►│                               │
  │   {public_key, x25519_public,  │                               │
  │    name}                       │                               │
  │◄── {device_code, auth_url} ────┤                               │
  │                               │                               │
  │   (display auth_url to user)  │                               │
  │                               │◄── GET /devices/authorize ─────┤
  │                               │    (user approves device)      │
  │                               ├── approval response ──────────►│
  │                               │                               │
  ├── GET /api/devices/status ────►│                               │
  │   ?code={device_code}          │                               │
  │◄── {status: "approved"} ──────┤                               │
```

### 3.3 Device Config Storage

```python
@dataclass
class DeviceConfig:
    device_id: str
    device_name: str
    private_key_b64: str       # Ed25519 private key
    public_key_b64: str        # Ed25519 public key
    base_url: str
    x25519_private_b64: str    # X25519 private key for ECDH
    x25519_public_b64: str     # X25519 public key for ECDH
    encryption_key_b64: str    # Canonical 32-byte encryption key
    workers: dict              # Worker process state
```

Store with atomic write (temp file → `os.replace`) and `0o600` permissions.

---

## 4. Key Exchange (Rekey) Protocol

This is how a new browser session obtains the encryption key from the device.

```
Browser                         Server                         Device
  │                               │                               │
  ├── Generate ephemeral X25519 ──┤                               │
  │   keypair                     │                               │
  │                               │                               │
  ├── POST /c/{ch}/rekey ─────────►│                               │
  │   {browser_x25519_public,     ├── notify device ─────────────►│
  │    request_id}                │   (aichat:rekey-request)      │
  │                               │                               │
  │   Meanwhile, browser:        │                               │
  │   shared = scalarMult(       │                               │
  │     browserPrivate,           │   Device:                     │
  │     deviceX25519Public)       │   shared = X25519.exchange(   │
  │   transportKey = HKDF(shared) │     devicePrivate,            │
  │                               │     browserPublic)            │
  │                               │   transportKey = HKDF(shared) │
  │                               │                               │
  │                               │   Wrap encryption key:        │
  │                               │   ct, nonce = SecretBox(      │
  │                               │     transportKey,             │
  │                               │     encryptionKey as UTF-8    │
  │                               │     base64 string)            │
  │                               │                               │
  │                               │◄── WS: rekey_response ────────┤
  │◄── SSE: aichat:rekey-response─┤   {encrypted_key, nonce,      │
  │                               │    request_id}                │
  │                               │                               │
  │   Unwrap:                     │                               │
  │   encKeyB64 = SecretBox.open( │                               │
  │     ct, nonce, transportKey)  │                               │
  │   encKey = base64Decode(      │                               │
  │     encKeyB64)                │                               │
  │                               │                               │
  │   Store encKey in localStorage│                               │
```

### Key Implementation Details

**Python (device side):**
```python
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
import nacl.secret, nacl.utils

def compute_shared_secret(our_private_b64, their_public_b64):
    private_key = X25519PrivateKey.from_private_bytes(base64.b64decode(our_private_b64))
    public_key = X25519PublicKey.from_public_bytes(base64.b64decode(their_public_b64))
    return private_key.exchange(public_key)

def derive_key(shared_secret):
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"aichat-device-key",
        info=b"v1",
    )
    return hkdf.derive(shared_secret)

# During rekey:
shared = compute_shared_secret(device_x25519_private_b64, browser_x25519_public_b64)
transport_key = derive_key(shared)
transport_key_b64 = base64.b64encode(transport_key).decode()

# Wrap the canonical encryption key with the transport key
ct, nonce = encrypt(transport_key_b64, encryption_key_b64)  # encryption_key_b64 is the plaintext here
# Send ct + nonce to browser
```

**JavaScript (browser side):**
```javascript
// Generate ephemeral keypair
var browserKP = nacl.box.keyPair();
var devicePub = nacl.util.decodeBase64(deviceX25519PublicB64);

// Compute shared secret via X25519
var sharedSecret = nacl.scalarMult(browserKP.secretKey, devicePub);

// Derive transport key via HKDF-SHA256 (Web Crypto API)
var salt = new TextEncoder().encode("aichat-device-key");
var info = new TextEncoder().encode("v1");
var keyMaterial = await crypto.subtle.importKey("raw", sharedSecret, "HKDF", false, ["deriveBits"]);
var derived = await crypto.subtle.deriveBits(
    { name: "HKDF", hash: "SHA-256", salt: salt, info: info },
    keyMaterial, 256
);
var transportKey = new Uint8Array(derived);

// Send browserKP.publicKey to device, receive encrypted_key + nonce back
// Unwrap:
var ct = nacl.util.decodeBase64(encryptedKeyB64);
var nonce = nacl.util.decodeBase64(nonceB64);
var unwrapped = nacl.secretbox.open(ct, nonce, transportKey);
var encKeyB64 = nacl.util.encodeUTF8(unwrapped);
var encryptionKey = nacl.util.decodeBase64(encKeyB64);

// Store for future use
localStorage.setItem("aichat:device_master_key", nacl.util.encodeBase64(encryptionKey));
```

---

## 5. Message Encryption & Decryption

### 5.1 Payload Schema

All encrypted content uses a versioned JSON envelope before encryption:

```json
{
  "schema": "aichat-e2e-v1",
  "meta": {
    "channel_id": "ch-abc123",
    "message_id": "msg-456"
  },
  "content": "Hello, world!",
  "attachments": []
}
```

- `channel_id` is **required** — prevents cross-channel replay attacks
- `message_id` is **optional** — included when the server-assigned ID is known, prevents cross-message replay

### 5.2 Encrypt

```python
import nacl.secret, nacl.utils, base64

def encrypt(key_b64: str, plaintext: str) -> tuple[str, str]:
    key = base64.b64decode(key_b64)  # 32 bytes
    box = nacl.secret.SecretBox(key)
    nonce = nacl.utils.random(24)  # random 24-byte nonce
    encrypted = box.encrypt(plaintext.encode("utf-8"), nonce)
    ciphertext = encrypted.ciphertext  # excludes prepended nonce
    return (
        base64.b64encode(ciphertext).decode(),
        base64.b64encode(nonce).decode(),
    )
```

### 5.3 Decrypt

```python
def decrypt(key_b64: str, ciphertext_b64: str, nonce_b64: str) -> str:
    key = base64.b64decode(key_b64)
    box = nacl.secret.SecretBox(key)
    ciphertext = base64.b64decode(ciphertext_b64)
    nonce = base64.b64decode(nonce_b64)
    plaintext = box.decrypt(ciphertext, nonce)
    return plaintext.decode("utf-8")
```

### 5.4 JavaScript Equivalent

```javascript
// Encrypt
function encrypt(plaintext) {
    var msg = nacl.util.decodeUTF8(plaintext);
    var nonce = nacl.randomBytes(24);
    var ct = nacl.secretbox(msg, nonce, channelKey);  // channelKey is Uint8Array(32)
    return {
        encrypted_payload: nacl.util.encodeBase64(ct),
        nonce: nacl.util.encodeBase64(nonce),
    };
}

// Decrypt
function decrypt(ciphertextB64, nonceB64) {
    var ct = nacl.util.decodeBase64(ciphertextB64);
    var nonce = nacl.util.decodeBase64(nonceB64);
    var plain = nacl.secretbox.open(ct, nonce, channelKey);
    if (!plain) return null;  // auth failed
    return nacl.util.encodeUTF8(plain);
}
```

---

## 6. Message Flow

### 6.1 Sending (Device → Server → Browser)

```
Worker (plaintext)
  │
  ├── Build versioned payload JSON
  ├── Encrypt with canonical key → (ciphertext_b64, nonce_b64)
  │
  ├── WS: send_message {
  │     channel_id, sender,
  │     content: "",              ← empty when encrypted
  │     encrypted_payload, nonce
  │   }
  │
  ├── Server stores content="[encrypted]" in DB
  ├── Server returns message_id
  │
  ├── Re-encrypt with message_id bound in metadata
  ├── WS: relay_content {
  │     channel_id, message_id,
  │     encrypted_payload, nonce   ← with message_id in metadata
  │   }
  │
  └── Server relays to browser via SSE (ephemeral, not persisted)
```

**Why two sends?** The first `send_message` creates the DB record and gets a `message_id`. The second `relay_content` includes the `message_id` in the encrypted metadata for replay protection. The relay is ephemeral — it's not stored server-side.

### 6.2 Receiving (Browser → Server → Device)

```
Browser
  ├── Encrypt plaintext with canonical key
  ├── POST /c/{channel_id}/send {encrypted_payload, nonce}
  │
Server
  ├── Store content="[encrypted]" in DB
  ├── Push encrypted payload directly to device via WebSocket
  │   (bypasses notification system — ciphertext never persisted)
  │
Device
  ├── Decrypt with canonical key
  ├── Validate channel_id and message_id in metadata
  ├── Store plaintext in local SQLite
  └── Forward plaintext to worker via IPC
```

### 6.3 History Requests

When a browser needs message history, it requests from the device (not the server), since the server only has `"[encrypted]"`:

```
Browser ──► Server ──► Device
                        ├── Query local SQLite for plaintext
                        ├── Encrypt each message with canonical key
                        └── Return encrypted messages
                   ◄────────────────────────────────────────
           ◄────────────
Browser decrypts each message locally
```

---

## 7. Server Implementation

The server is a zero-knowledge relay. Key responsibilities:

### 7.1 Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /api/devices/register` | Accept Ed25519 + X25519 public keys, return device code |
| `GET /api/devices/status` | Poll for device approval |
| `POST /c/{channel_id}/send` | Accept encrypted payload, store `"[encrypted]"`, relay to device |
| `POST /c/{channel_id}/rekey` | Forward browser's X25519 public key to device |

### 7.2 WebSocket Messages (device connection)

| Type | Direction | Purpose |
|------|-----------|---------|
| `send_message` | Device → Server | Send encrypted message, get message_id back |
| `relay_content` | Device → Server → Browser | Ephemeral encrypted content delivery |
| `rekey_response` | Device → Server → Browser | Deliver wrapped encryption key |
| `register_channel_key` | Device → Server | Persist encrypted channel key on channel record |
| `update_device_x25519` | Device → Server | Update X25519 public key |

### 7.3 Data Models

```sql
-- Device
CREATE TABLE ai_chat_device (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    public_key TEXT NOT NULL,           -- Ed25519 public (base64, 32 bytes)
    x25519_public TEXT,                 -- X25519 public (base64, 32 bytes)
    owner_user_id UUID NOT NULL,
    status TEXT NOT NULL,               -- pending, approved, denied
    last_seen_at TIMESTAMP
);

-- Channel
CREATE TABLE ai_chat_channel (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    public_key TEXT NOT NULL,           -- Ed25519 public (base64)
    device_id UUID REFERENCES ai_chat_device(id),
    encrypted_channel_key TEXT,         -- Encryption key wrapped with device master key
    key_nonce TEXT,                     -- Nonce for above
    archived BOOLEAN DEFAULT FALSE
);

-- Message
CREATE TABLE ai_chat_message (
    id UUID PRIMARY KEY,
    sender TEXT NOT NULL,               -- "user", "claude", "event"
    content TEXT NOT NULL,              -- "[encrypted]" when E2E active
    channel_id UUID REFERENCES ai_chat_channel(id),
    attachments JSONB,
    read_by_user_at TIMESTAMP,
    read_by_claude_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### 7.4 What the Server Stores

The server stores `encrypted_channel_key` and `key_nonce` on the channel record. This is the canonical encryption key wrapped with the device's master key — only the device can decrypt it. This allows the device to recover its encryption key from the server if local config is lost.

---

## 8. Request Authentication (Separate from E2E)

API requests are authenticated with Ed25519 signatures. This is orthogonal to encryption.

```
Signature = Ed25519.sign(private_key, f"{timestamp}.{METHOD}.{path}")

Headers:
  X-Timestamp: {unix_timestamp}
  X-Signature: {base64_signature}
  X-Channel: {channel_id}        (for channel-key auth)
  X-Device-Id: {device_id}       (for device-key auth)
```

- Server verifies signature against stored public key
- Timestamp must be within 60 seconds of server time
- Dual auth: try channel key first, fall back to device key

---

## 9. Security Properties

| Property | Status | Notes |
|----------|--------|-------|
| Confidentiality | ✅ | XSalsa20-Poly1305 authenticated encryption |
| Integrity | ✅ | Poly1305 MAC detects tampering |
| Server zero-knowledge | ✅ | Server never has encryption key or plaintext |
| Channel binding | ✅ | channel_id in encrypted metadata prevents cross-channel replay |
| Message binding | ✅ | message_id in metadata (when available) prevents cross-message replay |
| Forward secrecy | ⚠️ Limited | Transport keys are ephemeral, but canonical key is static |
| Key rotation | ❌ | Not implemented — simplicity trade-off |
| Local storage | ⚠️ | Plaintext stored in local SQLite; relies on OS access controls |

---

## 10. Dependencies

### Python (Device/Agent)

```
cryptography>=41.0    # X25519, HKDF, Ed25519
PyNaCl>=1.5           # SecretBox (XSalsa20-Poly1305)
```

### JavaScript (Browser)

```html
<!-- TweetNaCl + utils for base64/UTF-8 helpers -->
<script src="tweetnacl.min.js"></script>
<script src="tweetnacl-util.min.js"></script>
<!-- Web Crypto API is built into browsers (for HKDF) -->
```

---

## 11. Implementation Checklist

### Phase 1: Core Crypto Module
- [ ] Implement `generate_x25519_keypair()` — returns `{private_key_b64, public_key_b64}`
- [ ] Implement `encrypt(key_b64, plaintext)` → `(ciphertext_b64, nonce_b64)` using NaCl SecretBox
- [ ] Implement `decrypt(key_b64, ciphertext_b64, nonce_b64)` → plaintext string
- [ ] Implement ECDH: `compute_shared_secret(our_private_b64, their_public_b64)` → raw bytes
- [ ] Implement `derive_key(shared_secret)` → 32-byte key via HKDF-SHA256
- [ ] Write `KeyExchange` class with `complete_exchange()` and `restore_key()`
- [ ] Write comprehensive tests (roundtrip, wrong key, unicode, large payloads, tampered ciphertext)

### Phase 2: Device Auth & Config
- [ ] Implement Ed25519 keypair generation for device identity
- [ ] Design config storage format (JSON with restricted file permissions)
- [ ] Implement atomic save (temp file → rename) with `0o600` permissions
- [ ] Generate random 32-byte encryption key on first startup

### Phase 3: Server Endpoints
- [ ] `POST /api/devices/register` — accept public keys, create device code
- [ ] `GET /api/devices/status` — poll for approval
- [ ] Device authorization page — ECDH in browser during approval
- [ ] `POST /c/{channel_id}/send` — accept encrypted payload, store `"[encrypted]"`, relay
- [ ] `POST /c/{channel_id}/rekey` — forward browser X25519 public key to device
- [ ] WebSocket handler for device connections (send_message, relay_content, rekey_response)

### Phase 4: Device Manager Integration
- [ ] On startup: load or generate encryption key, init KeyExchange
- [ ] Handle rekey requests: ECDH → derive transport key → wrap encryption key → respond
- [ ] Outgoing messages: build versioned payload → encrypt → send → relay with message_id
- [ ] Incoming messages: decrypt → validate metadata → store plaintext locally → forward to worker
- [ ] History requests: query local DB → encrypt each → return

### Phase 5: Browser Integration
- [ ] Include tweetnacl + tweetnacl-util
- [ ] Implement crypto module (encrypt, decrypt, rekey flow)
- [ ] On page load: check localStorage for encryption key, trigger rekey if missing
- [ ] Encrypt outgoing messages before POST
- [ ] Decrypt incoming messages (SSE events) before rendering
- [ ] Handle rekey-response events to unwrap and store new key
- [ ] Show "rekey needed" UI when key is unavailable

### Phase 6: Encrypted Payload Versioning
- [ ] Use `{"schema": "your-app-e2e-v1", "meta": {...}, "content": "...", "attachments": [...]}` format
- [ ] Validate channel_id and message_id in metadata on decryption
- [ ] Support legacy payloads without schema/meta for backward compatibility
