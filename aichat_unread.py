#!/usr/bin/env python3
"""Read unread AI chat messages from the user."""

import os

import httpx

from aichat_auth import get_private_key, sign_request

URL = os.environ.get("AICHAT_URL", "https://aichat.zech.sh")


def main() -> None:
    private_key = get_private_key()

    path = "/api/messages/unread"
    headers = sign_request("GET", path, private_key=private_key)

    resp = httpx.get(f"{URL}{path}", headers=headers)
    resp.raise_for_status()

    messages = resp.json()
    if not messages:
        print("No unread messages.")
        return

    for msg in messages:
        print(f"[{msg['created_at']}] {msg['content']}")


if __name__ == "__main__":
    main()
