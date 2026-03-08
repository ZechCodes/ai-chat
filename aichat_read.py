#!/usr/bin/env python3
"""Read recent AI chat messages."""

import os
import sys

import httpx

from aichat_auth import get_private_key, sign_request

URL = os.environ.get("AICHAT_URL", "https://aichat.zech.sh")


def main() -> None:
    private_key = get_private_key()

    params = {}
    if len(sys.argv) > 1:
        params["limit"] = sys.argv[1]

    path = "/api/messages"
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        full_path = f"{path}?{query}"
    else:
        full_path = path

    headers = sign_request("GET", full_path, private_key=private_key)

    resp = httpx.get(f"{URL}{full_path}", headers=headers)
    resp.raise_for_status()

    for msg in resp.json():
        sender = msg["sender"].upper()
        read = " [read]" if msg.get("read_by_claude_at") else ""
        print(f"[{msg['created_at']}] {sender}{read}: {msg['content']}")


if __name__ == "__main__":
    main()
