"""SDK hook factories for reporting tool use to AI.CHAT."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aichat_api import AiChatAPI
    from aichat_interactions import InteractionManager

log = logging.getLogger(__name__)

# Tools that trigger the interaction overlay instead of normal execution
INTERACTION_TOOLS = {"AskUserQuestion", "EnterPlanMode"}


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


def make_interaction_hook(api: AiChatAPI, interactions: InteractionManager):
    """Create a PreToolUse hook that intercepts AskUserQuestion and EnterPlanMode.

    Sends the interaction to the web UI via the server and blocks until
    the user responds (approve/deny/answer).
    """

    async def hook(input_data, tool_use_id, context):
        tool_name = input_data.get("tool_name", "")
        tool_input = input_data.get("tool_input", {})

        if tool_name == "AskUserQuestion":
            # Extract question text and options from the questions array
            questions = tool_input.get("questions", [])
            if questions:
                q = questions[0]
                question = q.get("question", "")
                options = q.get("options", [])
                multi_select = q.get("multiSelect", False)
            else:
                question = tool_input.get("question", "")
                options = []
                multi_select = False
            log.info("Intercepted AskUserQuestion: %s", question[:100])

            await api.send_tool_status("active", tool="AskUserQuestion", description="Asking question...")
            result = await api.create_interaction("question", question, options=options, multi_select=multi_select)
            interaction_id = result.get("interaction_id")

            if not interaction_id:
                return {"decision": "block", "reason": "Failed to create interaction"}

            response = await interactions.wait_for_response(interaction_id)
            await api.send_tool_status("done", tool="AskUserQuestion")

            answer = response.get("answer", "")
            if response.get("action") == "deny":
                return {"decision": "block", "reason": "User declined to answer the question."}

            return {"decision": "block", "reason": f"User answered: {answer}"}

        if tool_name == "EnterPlanMode":
            plan = tool_input.get("plan", tool_input.get("description", ""))
            log.info("Intercepted EnterPlanMode")

            await api.send_tool_status("active", tool="EnterPlanMode", description="Awaiting plan approval...")
            result = await api.create_interaction("plan", plan)
            interaction_id = result.get("interaction_id")

            if not interaction_id:
                return {"decision": "block", "reason": "Failed to create interaction"}

            response = await interactions.wait_for_response(interaction_id)
            await api.send_tool_status("done", tool="EnterPlanMode")

            if response.get("action") == "accept":
                return {}  # Allow plan mode

            reason = response.get("reason", "User rejected the plan.")
            return {"decision": "block", "reason": reason}

        return {}  # Not an interaction tool, pass through

    return hook


def make_pre_tool_hook(api: AiChatAPI):
    """Create a PreToolUse hook that reports active status."""

    async def hook(input_data, tool_use_id, context):
        tool_name = input_data.get("tool_name", "")
        tool_input = input_data.get("tool_input", {})

        # Skip interaction tools (handled by interaction hook) and own API calls
        if tool_name in INTERACTION_TOOLS:
            return {}
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

        if tool_name in INTERACTION_TOOLS:
            return {}
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
