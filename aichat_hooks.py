"""SDK hook factories for reporting tool use to AI.CHAT."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aichat_api import AiChatAPI


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


def _is_own_api_call(tool_name: str, tool_input: dict) -> bool:
    """Check if the tool use is one of our own API calls."""
    if tool_name == "Bash":
        cmd = tool_input.get("command", "")
        if "aichat_" in cmd or "aichat-" in cmd or "relay_hook" in cmd:
            return True
    return False


def make_pre_tool_hook(api: AiChatAPI):
    """Create a PreToolUse hook that reports active status."""

    async def hook(input_data, tool_use_id, context):
        tool_name = input_data.get("tool_name", "")
        tool_input = input_data.get("tool_input", {})

        if _is_own_api_call(tool_name, tool_input):
            return {}

        description = _describe_tool(tool_name, tool_input)
        await api.send_tool_status("active", tool=tool_name, description=description)
        return {}

    return hook


def make_post_tool_hook(api: AiChatAPI):
    """Create a PostToolUse hook that reports done status."""

    async def hook(input_data, tool_use_id, context):
        tool_name = input_data.get("tool_name", "")
        tool_input = input_data.get("tool_input", {})

        if _is_own_api_call(tool_name, tool_input):
            return {}

        await api.send_tool_status("done", tool=tool_name)
        return {}

    return hook


def make_stop_hook(api: AiChatAPI):
    """Create a Stop hook that reports idle status."""

    async def hook(input_data, tool_use_id, context):
        await api.send_tool_status("idle")
        return {}

    return hook
