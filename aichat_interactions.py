"""Manages pending interactions between agent hooks and SSE responses."""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)


class InteractionManager:
    """Bridges agent hooks (which need to block) with SSE responses (which arrive async).

    When a hook intercepts AskUserQuestion or EnterPlanMode, it:
    1. Sends the interaction to the server via API
    2. Calls wait_for_response() which blocks on an asyncio.Event
    3. The SSE listener calls deliver_response() when the user responds
    4. The Event is set and the hook unblocks with the response

    If a new user message arrives while an interaction is pending, all pending
    interactions are auto-denied to unblock the agent and let it process the
    new message.
    """

    def __init__(self):
        self._pending: dict[str, asyncio.Event] = {}
        self._responses: dict[str, dict] = {}

    async def wait_for_response(self, interaction_id: str) -> dict:
        """Block until the SSE listener delivers a response (waits indefinitely).

        Also unblocks if cancel_all() is called (e.g. when a new user message
        arrives), returning a deny response.
        """
        event = asyncio.Event()
        self._pending[interaction_id] = event
        try:
            await event.wait()
            return self._responses.pop(interaction_id, {"action": "deny"})
        finally:
            self._pending.pop(interaction_id, None)

    def deliver_response(self, interaction_id: str, response: dict) -> None:
        """Called by SSE listener when a user response arrives."""
        self._responses[interaction_id] = response
        event = self._pending.get(interaction_id)
        if event:
            log.info("Delivering interaction response for %s", interaction_id)
            event.set()
        else:
            log.warning("No pending interaction for %s", interaction_id)

    def cancel_all(self) -> None:
        """Cancel all pending interactions (auto-deny).

        Called when a new user message arrives to unblock any waiting hooks
        so the agent can process the message instead.
        """
        if not self._pending:
            return
        log.info("Cancelling %d pending interaction(s) due to new message", len(self._pending))
        for interaction_id, event in list(self._pending.items()):
            self._responses.setdefault(
                interaction_id,
                {"action": "deny", "reason": "Cancelled — new message received"},
            )
            event.set()
