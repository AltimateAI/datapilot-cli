import asyncio
import json
import logging
import shutil

import click
from mcp import ClientSession
from mcp import StdioServerParameters
from mcp.client.stdio import stdio_client

logging.basicConfig(level=logging.INFO)

def find_input_tokens(data):
    tokens = set()
    if isinstance(data, list):
        for item in data:
            tokens.update(find_input_tokens(item))
    elif isinstance(data, dict):
        for value in data.values():
            tokens.update(find_input_tokens(value))
    elif isinstance(data, str) and data.startswith("${input:"):
        tokens.add(data[8:-1].strip())
    return tokens


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

    # Collect all input IDs from config and server templates
    input_ids = set()
    input_ids.update(find_input_tokens(mcp_config.get("servers", {})))
    input_ids.update(input_def["id"] for input_def in mcp_config.get("inputs", []))
    
    # Create prompt definitions merging config and discovered inputs
    for input_id in input_ids:
        input_def = next((d for d in mcp_config.get("inputs", []) if d["id"] == input_id), {})
        inputs[input_id] = click.prompt(
            input_def.get("description", input_id), 
            hide_input=input_def.get("password", False)
        )

    # Select server
    servers = mcp_config.get("servers", {})
    server_names = list(servers.keys())
    
    if not server_names:
        raise click.UsageError("No servers configured in mcp config")
        
    if len(server_names) > 1:
        server_name = click.prompt(
            "Choose a server",
            type=click.Choice(server_names),
            show_choices=True
        )
    else:
        server_name = server_names[0]
    
    if server_name in servers:
        server_config = servers[server_name]
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
        output_with_name = {"name": server_name, **output}
        click.echo(json.dumps(output_with_name, indent=2))


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
