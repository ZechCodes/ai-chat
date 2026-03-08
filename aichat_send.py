#!/usr/bin/env python3
"""Send a message as Claude to AI chat."""

import os
import sys

import httpx

from aichat_auth import get_private_key, sign_request

URL = os.environ.get("AICHAT_URL", "https://aichat.zech.sh")


def main() -> None:
    private_key = get_private_key()

    if len(sys.argv) < 2:
        print("Usage: aichat_send.py <message>", file=sys.stderr)
        sys.exit(1)

    content = " ".join(sys.argv[1:])
    path = "/api/messages"

    headers = sign_request("POST", path, private_key=private_key)
    headers["Content-Type"] = "application/json"

    resp = httpx.post(f"{URL}{path}", json={"content": content}, headers=headers)
    resp.raise_for_status()
    print(f"Sent: {resp.json()}")


if __name__ == "__main__":
    main()
