#!/usr/bin/env python3
"""Claude Code hook script that relays tool use events to aichat."""

import json
import os
import sys

import httpx

from aichat_auth import get_private_key, has_token, sign_request

URL = os.environ.get("AICHAT_URL", "https://aichat.zech.sh")


def _describe_tool(tool_name: str, tool_input: dict) -> str:
    """Generate a human-readable description of what the tool is doing."""
    if tool_name == "Read":
        path = tool_input.get("file_path", "")
        return f"Reading {os.path.basename(path)}" if path else "Reading file"
    if tool_name == "Edit":
        path = tool_input.get("file_path", "")
        return f"Editing {os.path.basename(path)}" if path else "Editing file"
    if tool_name == "Write":
        path = tool_input.get("file_path", "")
        return f"Writing {os.path.basename(path)}" if path else "Writing file"
    if tool_name == "Bash":
        cmd = tool_input.get("command", "")
        # Truncate long commands
        if len(cmd) > 60:
            cmd = cmd[:57] + "..."
        return f"Running: {cmd}" if cmd else "Running command"
    if tool_name == "Glob":
        pattern = tool_input.get("pattern", "")
        return f"Searching for {pattern}" if pattern else "Searching files"
    if tool_name == "Grep":
        pattern = tool_input.get("pattern", "")
        return f"Searching for '{pattern}'" if pattern else "Searching code"
    if tool_name == "WebFetch":
        return "Fetching web page"
    if tool_name == "WebSearch":
        query = tool_input.get("query", "")
        return f"Searching: {query}" if query else "Web search"
    if tool_name == "Agent":
        desc = tool_input.get("description", "")
        return f"Agent: {desc}" if desc else "Running agent"
    return f"Using {tool_name}"


def main() -> None:
    if not has_token():
        return  # No token for this project, skip silently

    event_type = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    payload = json.load(sys.stdin)

    tool_name = payload.get("tool_name", "")
    tool_input = payload.get("tool_input", {})

    # Skip sending tool status for our own API calls to avoid loops
    if tool_name == "Bash":
        cmd = tool_input.get("command", "")
        if "aichat_" in cmd or "relay_hook" in cmd:
            return

    body = {}
    if event_type == "prompt_submit":
        body = {
            "status": "active",
            "tool": "thinking",
            "description": "Thinking...",
        }
    elif event_type == "pre_tool_use":
        body = {
            "status": "active",
            "tool": tool_name,
            "description": _describe_tool(tool_name, tool_input),
        }
    elif event_type == "post_tool_use":
        body = {
            "status": "done",
            "tool": tool_name,
        }
    elif event_type == "subagent_start":
        desc = payload.get("description", "")
        body = {
            "status": "active",
            "tool": "subagent",
            "description": f"Agent: {desc}" if desc else "Running agent...",
        }
    elif event_type == "subagent_stop":
        body = {
            "status": "done",
            "tool": "subagent",
        }
    elif event_type == "stop":
        body = {"status": "idle"}
    else:
        return

    try:
        private_key = get_private_key()
        path = "/api/tool-status"
        headers = sign_request("POST", path, private_key=private_key)
        headers["Content-Type"] = "application/json"
        httpx.post(f"{URL}{path}", json=body, headers=headers, timeout=5)
    except Exception:
        pass  # Fire and forget — never block Claude


if __name__ == "__main__":
    main()
