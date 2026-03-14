#!/usr/bin/env python3
"""Standalone MCP server for Codex workers.

Exposes the same messaging tools as aichat_agent.py's in-process MCP
(send, read_unread, set_directories) over stdio. Codex spawns this as
a subprocess and communicates via JSON-RPC.

The server can talk to AI.CHAT through either:
  - IPC (preferred when launched by aichat_manager)
  - Direct API auth (token or device key), for standalone codex_agent usage
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
BASE_URL = os.environ.get("AICHAT_BASE_URL")
TOKEN = os.environ.get("AICHAT_TOKEN")
DEVICE_KEY = os.environ.get("AICHAT_DEVICE_KEY")
DEVICE_ID = os.environ.get("AICHAT_DEVICE_ID")

# Global backend client (IPCClient or AiChatAPI)
_api_client = None


async def get_api_client():
    """Lazily connect to IPC or construct a direct API client."""
    global _api_client
    if _api_client is not None:
        return _api_client

    if IPC_SOCKET:
        if not CHANNEL_ID:
            raise RuntimeError("AICHAT_CHANNEL_ID is required when using AICHAT_IPC_SOCKET")
        from aichat_ipc import IPCClient

        client = IPCClient(IPC_SOCKET, CHANNEL_ID, role="auxiliary")
        await client.connect()
        _api_client = client
        log.info("Connected to IPC at %s for channel %s", IPC_SOCKET, CHANNEL_ID)
        return client

    if not TOKEN and not DEVICE_KEY:
        raise RuntimeError(
            "Provide AICHAT_IPC_SOCKET or direct auth env vars (AICHAT_TOKEN or AICHAT_DEVICE_KEY)"
        )
    from aichat_api import AiChatAPI

    _api_client = AiChatAPI(
        base_url=BASE_URL,
        token=TOKEN,
        device_key=DEVICE_KEY,
        device_id=DEVICE_ID,
        channel_id=CHANNEL_ID or None,
    )
    log.info("Using direct API client for channel %s", getattr(_api_client, "channel_id", CHANNEL_ID or "?"))
    return _api_client


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
                    "Call this when you start working in a new directory or need access to additional project roots."
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
            api = await get_api_client()
        except Exception as e:
            return [TextContent(type="text", text=f"AI.CHAT connection error: {e}")]

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
            try:
                messages = await api.get_unread_messages()
            except Exception as e:
                return [TextContent(type="text", text=f"Failed to read unread: {e}")]

            if not messages:
                return [TextContent(type="text", text="No unread messages.")]

            ids_to_mark = [
                m.get("id") or m.get("message_id")
                for m in messages
                if m.get("id") or m.get("message_id")
            ]
            if ids_to_mark:
                try:
                    await api.mark_read(ids_to_mark)
                except Exception as e:
                    log.warning("Failed to mark messages read: %s", e)

            lines = []
            for msg in messages:
                content = msg.get("content", "")
                attachments = msg.get("attachments", [])
                att_note = f" [{len(attachments)} attachment(s)]" if attachments else ""
                lines.append(f"Zech: {content}{att_note}")

            return [TextContent(type="text", text="\n".join(lines))]

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
