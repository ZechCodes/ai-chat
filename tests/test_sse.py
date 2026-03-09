"""Tests for SSE message parsing logic."""

import json
import pytest

from aichat_agent import _parse_sse_event


class TestParseSSEEvent:
    def test_parses_user_message(self):
        data = json.dumps({
            "type": "aichat:message",
            "sender": "user",
            "channel_id": "ch-1",
            "content": "Hello agent",
        })
        line = f"data: {data}"
        result = _parse_sse_event(line, "ch-1")
        assert result == {"content": "Hello agent", "attachments": []}

    def test_ignores_claude_messages(self):
        data = json.dumps({
            "type": "aichat:message",
            "sender": "claude",
            "channel_id": "ch-1",
            "content": "I am claude",
        })
        line = f"data: {data}"
        assert _parse_sse_event(line, "ch-1") is None

    def test_ignores_other_channels(self):
        data = json.dumps({
            "type": "aichat:message",
            "sender": "user",
            "channel_id": "ch-2",
            "content": "wrong channel",
        })
        line = f"data: {data}"
        assert _parse_sse_event(line, "ch-1") is None

    def test_ignores_non_message_events(self):
        data = json.dumps({
            "type": "aichat:tool-status",
            "channel_id": "ch-1",
        })
        line = f"data: {data}"
        assert _parse_sse_event(line, "ch-1") is None

    def test_ignores_non_data_lines(self):
        assert _parse_sse_event("event: message", "ch-1") is None
        assert _parse_sse_event(": heartbeat", "ch-1") is None
        assert _parse_sse_event("", "ch-1") is None

    def test_ignores_invalid_json(self):
        assert _parse_sse_event("data: not valid json", "ch-1") is None
