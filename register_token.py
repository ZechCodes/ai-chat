#!/usr/bin/env python3
"""Register an aichat token for a working directory.

Usage:
    uv run python3 register_token.py TOKEN [DIRECTORY]

If DIRECTORY is omitted, uses the current working directory.
Tokens are stored in ~/.config/aichat/tokens.json.
"""

import json
import os
import sys
from pathlib import Path

TOKENS_PATH = Path.home() / ".config" / "aichat" / "tokens.json"


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: register_token.py TOKEN [DIRECTORY]")
        raise SystemExit(1)

    token = sys.argv[1]
    directory = os.path.abspath(sys.argv[2] if len(sys.argv) > 2 else os.getcwd())

    # Load existing tokens
    tokens: dict[str, str] = {}
    if TOKENS_PATH.exists():
        tokens = json.loads(TOKENS_PATH.read_text())

    tokens[directory] = token

    # Write back
    TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKENS_PATH.write_text(json.dumps(tokens, indent=2) + "\n")

    print(f"Registered token for {directory}")
    print(f"Stored in {TOKENS_PATH}")


if __name__ == "__main__":
    main()
