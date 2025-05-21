import asyncio
import logging
import json
import shutil

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import click


logging.basicConfig(level=logging.INFO)

# New mcp group
@click.group()
def mcp():
    """mcp specific commands."""


@mcp.command("create-mcp-proxy")
def create_mcp_proxy():
    content = click.edit()
    if content is None:
        click.echo("No input provided.")

    output = asyncio.run(list_tools())
    click.echo(json.dumps(output, indent=2))

async def list_tools(command: str, args: list[str], env: dict[str, str]) -> str:
    command = shutil.which(command)

    # Create server parameters for stdio connection
    server_params = StdioServerParameters(
        command=command,  # Executable
        args=args,  # Optional command line arguments
        env=None,  # Optional environment variables
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(
            read, write
        ) as session:
            # Initialize the connection
            await session.initialize()

            # List available tools
            tools = await session.list_tools()

            # print as json
            tools_list = [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.inputSchema,
                }
                for tool in tools.tools
            ]

            return tools_list

