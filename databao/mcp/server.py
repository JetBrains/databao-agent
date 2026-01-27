"""MCP server for Databao."""

import asyncio
from typing import Any

import mcp.types as types
from mcp.server import InitializationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities, ToolsCapability

from databao.mcp.tools import TOOLS, DatabaoMCPTools


async def run_mcp_server() -> None:
    server = Server("databao")
    tools_handler = DatabaoMCPTools()

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return TOOLS

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
        if name == "visualize_data":
            return await tools_handler.handle_visualize_data(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")

    async with stdio_server() as (read_stream, write_stream):
        init_options = InitializationOptions(
            server_name="databao",
            server_version="1.0.0",
            capabilities=ServerCapabilities(
                tools=ToolsCapability(listChanged=True),
            ),
        )
        await server.run(read_stream, write_stream, init_options)


def main() -> None:
    asyncio.run(run_mcp_server())


if __name__ == "__main__":
    main()
