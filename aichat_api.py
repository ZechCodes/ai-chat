"""Unified async client for AI.CHAT API endpoints."""

import os
import tempfile

import httpx

from aichat_auth import get_channel_id, get_private_key, sign_request


class AiChatAPI:
    """Async client for AI.CHAT API endpoints."""

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        device_key: str | None = None,
        device_id: str | None = None,
        channel_id: str | None = None,
    ):
        self.base_url = (base_url or os.environ.get("AICHAT_URL", "https://aichat.zech.sh")).rstrip("/")
        self.device_id = device_id

        if device_key:
            # Device key auth: use device's key with channel_id
            import base64 as _b64
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
            key_bytes = _b64.b64decode(device_key)
            self._private_key = Ed25519PrivateKey.from_private_bytes(key_bytes)
            self.channel_id = channel_id
        else:
            # Legacy: compound token or tokens.json
            self._private_key = get_private_key(token=token)
            self.channel_id = channel_id or get_channel_id(token=token)

    def _sign_request(self, method: str, path: str) -> dict[str, str]:
        """Sign a request and return auth headers."""
        headers = sign_request(method, path, private_key=self._private_key, channel_id=self.channel_id)
        if self.device_id:
            headers["X-Device-Id"] = self.device_id
        return headers

    async def send_message(self, content: str) -> dict:
        """POST /api/messages"""
        path = "/api/messages"
        headers = self._sign_request("POST", path)
        headers["Content-Type"] = "application/json"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{self.base_url}{path}", json={"content": content}, headers=headers)
            resp.raise_for_status()
            return resp.json()

    async def mark_read(self, message_ids: list[str]) -> dict:
        """POST /api/messages/read"""
        if not message_ids:
            return {"marked": []}
        path = "/api/messages/read"
        headers = self._sign_request("POST", path)
        headers["Content-Type"] = "application/json"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.base_url}{path}",
                json={"message_ids": message_ids},
                headers=headers,
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json()

    async def send_tool_status(self, status: str, tool: str = "", description: str = "") -> None:
        """POST /api/tool-status"""
        path = "/api/tool-status"
        headers = self._sign_request("POST", path)
        headers["Content-Type"] = "application/json"
        body: dict = {"status": status}
        if tool:
            body["tool"] = tool
        if description:
            body["description"] = description
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{self.base_url}{path}", json=body, headers=headers, timeout=5)

    async def get_session(self) -> httpx.Cookies:
        """POST /api/session — exchange Ed25519 auth for a session cookie."""
        path = "/api/session"
        headers = self._sign_request("POST", path)
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{self.base_url}{path}", headers=headers)
            resp.raise_for_status()
            return resp.cookies

    async def create_interaction(
        self,
        interaction_type: str,
        content: str,
        *,
        options: list[dict] | None = None,
        multi_select: bool = False,
    ) -> dict:
        """POST /api/interaction — create a pending interaction (question or plan)."""
        path = "/api/interaction"
        headers = self._sign_request("POST", path)
        headers["Content-Type"] = "application/json"
        body: dict = {"type": interaction_type, "content": content}
        if options:
            body["options"] = options
            body["multi_select"] = multi_select
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{self.base_url}{path}", json=body, headers=headers)
            resp.raise_for_status()
            return resp.json()

    async def send_event(self, event_type: str) -> dict:
        """POST /api/event — fire-and-forget channel event."""
        path = "/api/event"
        headers = self._sign_request("POST", path)
        headers["Content-Type"] = "application/json"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.base_url}{path}",
                json={"event_type": event_type},
                headers=headers,
                timeout=5,
            )
            resp.raise_for_status()
            return resp.json()

    async def report_directories(
        self,
        working_directory: str,
        additional_directories: list[str] | None = None,
    ) -> dict:
        """POST /api/directories — report working directory to server."""
        path = "/api/directories"
        headers = self._sign_request("POST", path)
        headers["Content-Type"] = "application/json"
        body: dict = {"working_directory": working_directory}
        if additional_directories:
            body["additional_directories"] = additional_directories
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.base_url}{path}", json=body, headers=headers
            )
            resp.raise_for_status()
            return resp.json()

    async def download_attachment(self, url: str) -> str:
        """Download an attachment to a temp file, return the file path."""
        full_url = url if url.startswith("http") else f"{self.base_url}{url}"
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(full_url, timeout=30)
            resp.raise_for_status()

        # Determine extension from content-type
        ct = resp.headers.get("content-type", "")
        ext = ".png"
        if "jpeg" in ct or "jpg" in ct:
            ext = ".jpg"
        elif "gif" in ct:
            ext = ".gif"
        elif "webp" in ct:
            ext = ".webp"
        elif "svg" in ct:
            ext = ".svg"

        tmp = tempfile.NamedTemporaryFile(
            suffix=ext, prefix="aichat_img_", delete=False
        )
        tmp.write(resp.content)
        tmp.close()
        return tmp.name
