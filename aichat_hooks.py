"""SDK hook factories for reporting tool use to AI.CHAT."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from aichat_interactions import InteractionManager


class ChatAPI(Protocol):
    """Protocol for the API used by hooks — satisfied by both AiChatAPI and IPCClient."""

    async def send_tool_status(self, status: str, tool: str = "", description: str = "") -> None: ...
    async def create_interaction(self, interaction_type: str, content: str, *, options: list[dict] | None = None, multi_select: bool = False) -> dict: ...
    async def send_event(self, event_type: str) -> dict: ...

log = logging.getLogger(__name__)

# Tools that trigger the interaction overlay instead of normal execution
# AskUserQuestion is intercepted in PreToolUse; ExitPlanMode in can_use_tool
INTERACTION_TOOLS = {"AskUserQuestion", "EnterPlanMode", "ExitPlanMode"}


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


def _read_plan_file(tool_input: dict, last_written_file: str | None) -> str:
    """Read the plan content for ExitPlanMode approval.

    Plan files are written to ~/.claude/plans/ during plan mode. We find them by:
    1. Checking tool_input for a file path
    2. Using the last file written by the Write tool (tracked by hooks)
    3. Finding the most recently modified file in ~/.claude/plans/
    """
    # 1. Check tool_input for a path reference
    for key in ("plan_file", "file_path", "path"):
        path = tool_input.get(key)
        if path and os.path.isfile(path):
            return _read_file_content(path)

    # 2. Use last written file if tracked
    if last_written_file and os.path.isfile(last_written_file):
        return _read_file_content(last_written_file)

    # 3. Find most recent plan file in ~/.claude/plans/
    plans_dir = Path.home() / ".claude" / "plans"
    if plans_dir.is_dir():
        plan_files = sorted(plans_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
        if plan_files:
            return _read_file_content(str(plan_files[0]))

    log.warning("Could not find plan file; tool_input=%s", json.dumps(tool_input, default=str)[:300])
    return "Claude has finished designing an implementation plan and is requesting approval."


def _read_file_content(path: str) -> str:
    """Read a file's content, returning a fallback on error."""
    try:
        content = Path(path).read_text().strip()
        if content:
            log.info("Read plan from %s (%d chars)", path, len(content))
            return content
    except OSError as e:
        log.warning("Failed to read plan file %s: %s", path, e)
    return "Claude has finished designing an implementation plan and is requesting approval."


def _deny(input_data, reason: str):
    """Return a properly formatted deny decision for PreToolUse hooks."""
    return {
        "hookSpecificOutput": {
            "hookEventName": input_data.get("hook_event_name", "PreToolUse"),
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def make_interaction_hook(api: ChatAPI, interactions: InteractionManager):
    """Create a PreToolUse hook that intercepts AskUserQuestion.

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
                return _deny(input_data, "Failed to create interaction")

            response = await interactions.wait_for_response(interaction_id)
            await api.send_tool_status("done", tool="AskUserQuestion")

            answer = response.get("answer", "")
            if response.get("action") == "deny":
                return _deny(input_data, "User declined to answer the question.")

            return _deny(input_data, f"User answered: {answer}")

        return {"continue_": True}  # Not an interaction tool, pass through

    return hook


def make_can_use_tool(api: ChatAPI, interactions: InteractionManager, hook_state: dict):
    """Create a can_use_tool callback for ClaudeAgentOptions.

    The CLI dispatches ExitPlanMode (and other permission-gated tools) through
    the can_use_tool control request — NOT through PermissionRequest hooks.
    This callback handles ExitPlanMode approval via the interaction system and
    auto-allows everything else.

    hook_state is shared with PreToolUse/PostToolUse hooks to track the last
    file written (used to find the plan file).
    """
    from claude_agent_sdk import PermissionResultAllow, PermissionResultDeny, ToolPermissionContext

    async def callback(
        tool_name: str,
        tool_input: dict,
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        if tool_name == "EnterPlanMode":
            try:
                await api.send_event("plan:enter")
            except Exception:
                log.warning("Failed to send plan:enter event")
            return PermissionResultAllow()

        if tool_name != "ExitPlanMode":
            return PermissionResultAllow()

        log.info("Intercepted ExitPlanMode (can_use_tool), input keys=%s", list(tool_input.keys()))

        plan_content = _read_plan_file(tool_input, hook_state.get("last_written_file"))

        await api.send_tool_status("active", tool="ExitPlanMode", description="Awaiting plan approval...")
        result = await api.create_interaction("plan", plan_content)
        interaction_id = result.get("interaction_id")

        if not interaction_id:
            return PermissionResultDeny(message="Failed to create interaction")

        response = await interactions.wait_for_response(interaction_id)
        await api.send_tool_status("done", tool="ExitPlanMode")

        if response.get("action") == "accept":
            try:
                await api.send_event("plan:exit")
            except Exception:
                log.warning("Failed to send plan:exit event")
            return PermissionResultAllow()

        reason = response.get("reason") or "User rejected the plan."
        try:
            await api.send_event("plan:exit")
        except Exception:
            log.warning("Failed to send plan:exit event")
        return PermissionResultDeny(message=reason)

    return callback


def make_pre_tool_hook(api: ChatAPI, hook_state: dict):
    """Create a PreToolUse hook that reports active status and tracks writes."""

    async def hook(input_data, tool_use_id, context):
        tool_name = input_data.get("tool_name", "")
        tool_input = input_data.get("tool_input", {})

        # Track Write calls so can_use_tool can find the plan file
        if tool_name == "Write":
            path = tool_input.get("file_path", "")
            if path:
                hook_state["last_written_file"] = path

        # Skip interaction tools (handled by interaction hook) and own API calls
        if tool_name in INTERACTION_TOOLS:
            return {"continue_": True}
        if _is_own_api_call(tool_name, tool_input):
            return {"continue_": True}

        description = _describe_tool(tool_name, tool_input)
        await api.send_tool_status("active", tool=tool_name, description=description)
        return {"continue_": True}

    return hook


def _format_edit_diff(tool_input: dict) -> str | None:
    """Format an Edit tool's old_string/new_string as a unified diff description."""
    old = tool_input.get("old_string", "")
    new = tool_input.get("new_string", "")
    path = tool_input.get("file_path", "")
    if not old and not new:
        return None

    filename = os.path.basename(path) if path else "file"
    lines = [f"diff:{filename}"]
    old_lines = old.splitlines()
    new_lines = new.splitlines()

    for line in old_lines:
        lines.append(f"-{line}")
    for line in new_lines:
        lines.append(f"+{line}")

    return "\n".join(lines)


def make_post_tool_hook(api: ChatAPI):
    """Create a PostToolUse hook that reports done status."""

    async def hook(input_data, tool_use_id, context):
        tool_name = input_data.get("tool_name", "")
        tool_input = input_data.get("tool_input", {})

        if tool_name in INTERACTION_TOOLS:
            return {}
        if _is_own_api_call(tool_name, tool_input):
            return {}

        # Send edit diff before marking done
        if tool_name == "Edit":
            diff_desc = _format_edit_diff(tool_input)
            if diff_desc:
                await api.send_tool_status("active", tool="Edit", description=diff_desc)

        await api.send_tool_status("done", tool=tool_name)
        return {}

    return hook


def make_stop_hook(api: ChatAPI):
    """Create a Stop hook that reports idle status."""

    async def hook(input_data, tool_use_id, context):
        await api.send_tool_status("idle")
        return {}

    return hook


def make_pre_compact_hook(api: ChatAPI):
    """Create a PreCompact hook that notifies when context compaction starts."""

    async def hook(input_data, tool_use_id, context):
        trigger = input_data.get("trigger", "auto")
        await api.send_tool_status(
            "active", tool="compact", description=f"Compacting context ({trigger})..."
        )
        return {}

    return hook
