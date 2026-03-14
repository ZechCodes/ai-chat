#!/usr/bin/env python3
"""Standalone MCP server for Codex workers.

Exposes the same messaging tools as aichat_agent.py's in-process MCP
(send, read_unread, set_directories) over stdio. Codex spawns this as
a subprocess and communicates via JSON-RPC.

The server connects to the AI.CHAT manager via IPC using env vars:
  AICHAT_IPC_SOCKET — path to the manager's Unix domain socket
  AICHAT_CHANNEL_ID — channel this worker is associated with
"""

from __future__ import annotations

import asyncio
import logging
import os

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s mcp-server %(message)s")
log = logging.getLogger(__name__)

# Resolve config from environment
IPC_SOCKET = os.environ.get("AICHAT_IPC_SOCKET", "")
CHANNEL_ID = os.environ.get("AICHAT_CHANNEL_ID", "")

# Global IPC client (initialized in main)
_ipc_client = None


async def get_ipc_client():
    """Lazily connect to the IPC socket."""
    global _ipc_client
    if _ipc_client is not None:
        return _ipc_client

    if not IPC_SOCKET or not CHANNEL_ID:
        raise RuntimeError(
            "AICHAT_IPC_SOCKET and AICHAT_CHANNEL_ID env vars are required"
        )

    from aichat_ipc import IPCClient
    client = IPCClient(IPC_SOCKET, CHANNEL_ID)
    await client.connect()
    _ipc_client = client
    log.info("Connected to IPC at %s for channel %s", IPC_SOCKET, CHANNEL_ID)
    return client


def create_server() -> Server:
    server = Server("aichat")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="send",
                description="Send a message to the user via AI.CHAT",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "message": {
                            "type": "string",
                            "description": "The message to send",
                        }
                    },
                    "required": ["message"],
                },
            ),
            Tool(
                name="read_unread",
                description="Read unread messages from Zech. Call this when notified about unread messages.",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            Tool(
                name="set_directories",
                description=(
                    "Report your working directory and any additional project directories you are accessing. "
                    "Call this when you start working in a new directory."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "working_directory": {
                            "type": "string",
                            "description": "Primary working directory",
                        },
                        "additional_directories": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Additional project roots",
                        },
                    },
                    "required": ["working_directory"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        try:
            api = await get_ipc_client()
        except Exception as e:
            return [TextContent(type="text", text=f"IPC connection error: {e}")]

        if name == "send":
            message = arguments.get("message", "")
            if not message:
                return [TextContent(type="text", text="No message provided")]
            try:
                result = await api.send_message(message)
                return [TextContent(type="text", text=f"Message sent: {result.get('id', 'ok')}")]
            except Exception as e:
                return [TextContent(type="text", text=f"Failed to send: {e}")]

        elif name == "read_unread":
            # The Codex agent doesn't have a shared message queue like the
            # Claude agent does (where hooks inject messages mid-turn).
            # For now, return a note that the queue is managed externally.
            return [TextContent(type="text", text="No unread messages. Messages are delivered as new turns.")]

        elif name == "set_directories":
            working_dir = arguments.get("working_directory", "")
            additional = arguments.get("additional_directories", [])
            try:
                await api.report_directories(working_dir, additional or None)
                return [TextContent(type="text", text="Directories updated.")]
            except Exception as e:
                return [TextContent(type="text", text=f"Failed to update directories: {e}")]

        return [TextContent(type="text", text=f"Unknown tool: {name}")]

    return server


async def main():
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
