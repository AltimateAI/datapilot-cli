import asyncio
import json
import logging
import shutil

import click
from mcp import ClientSession
from mcp import StdioServerParameters
from mcp.client.stdio import stdio_client

logging.basicConfig(level=logging.INFO)


# New mcp group
@click.group()
def mcp():
    """mcp specific commands."""


@mcp.command("inspect-mcp-server")
def create_mcp_proxy():
    content = click.edit()
    if content is None:
        click.echo("No input provided.")
        return

    try:
        config = json.loads(content)
    except json.JSONDecodeError:
        click.echo("Invalid JSON content.")
        return

    inputs = {}
    mcp_config = config.get("mcp", {})

    # Process inputs first
    for input_def in mcp_config.get("inputs", []):
        input_id = input_def["id"]
        inputs[input_id] = click.prompt(input_def.get("description", input_id), hide_input=input_def.get("password", False))

    # Process servers
    servers = mcp_config.get("servers", {})
    for server_name, server_config in servers.items():
        # Replace input tokens in args
        processed_args = [
            inputs.get(arg[8:-1], arg) if isinstance(arg, str) and arg.startswith("${input:") else arg
            for arg in server_config.get("args", [])
        ]

        # Replace input tokens in environment variables
        processed_env = {
            k: inputs.get(v[8:-1], v) if isinstance(v, str) and v.startswith("${input:") else v
            for k, v in server_config.get("env", {}).items()
        }

        # Execute with processed parameters
        output = asyncio.run(list_tools(command=server_config["command"], args=processed_args, env=processed_env))
        click.echo(f"\nServer: {server_name}")
        click.echo(json.dumps(output, indent=2))


async def list_tools(command: str, args: list[str], env: dict[str, str]):
    command_path = shutil.which(command)
    if not command_path:
        raise click.UsageError(f"Command not found: {command}")

    server_params = StdioServerParameters(
        command=command_path,
        args=args,
        env=env,  # Now using processed env
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            mcp_tools = [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.inputSchema,
                }
                for tool in tools.tools
            ]

            return {
                "tools": mcp_tools,
            }
