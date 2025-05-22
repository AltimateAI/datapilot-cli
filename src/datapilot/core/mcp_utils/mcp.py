import asyncio
import json
import logging
import shutil
from dataclasses import dataclass

import click
import pyperclip
from mcp import ClientSession
from mcp import StdioServerParameters
from mcp.client.stdio import stdio_client

logging.basicConfig(level=logging.INFO)


@dataclass
class InputParameter:
    name: str
    type: str
    required: bool
    key: str
    description: str


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

    # Select server
    # Support both "servers" and "mcpServers" naming conventions
    servers = mcp_config.get("mcpServers", mcp_config.get("servers", {}))
    server_names = list(servers.keys())

    if not server_names:
        ctx = click.get_current_context()
        click.secho("Error: No servers configured in mcp config (tried keys: 'mcpServers' and 'servers')", fg="red")
        ctx.exit(1)

    if len(server_names) > 1:
        server_name = click.prompt("Choose a server", type=click.Choice(server_names), show_choices=True)
    else:
        server_name = server_names[0]

    if server_name in servers:
        server_config = servers[server_name]

        # Collect input tokens ONLY from this server's config
        input_ids = find_input_tokens(server_config.get("args", []))
        input_ids.update(find_input_tokens(server_config.get("env", {})))

        # Create prompt definitions using BOTH discovered tokens AND configured inputs
        existing_input_ids = {i["id"] for i in mcp_config.get("inputs", [])}
        inputs_to_prompt = input_ids.intersection(existing_input_ids)
        inputs_to_prompt.update(input_ids)  # Add any undiscovered-by-config inputs

        input_configs = []
        for input_id in inputs_to_prompt:
            input_def = next((d for d in mcp_config.get("inputs", []) if d["id"] == input_id), {})
            inputs[input_id] = click.prompt(
                input_def.get("description", input_id),
                hide_input=True,
            )
            # Create InputParameters config entry
            input_configs.append(
                InputParameter(
                    name=input_def.get("name", input_id),
                    type="password",
                    required=True,
                    key=input_id,
                    description=input_def.get("description", ""),
                ).__dict__
            )

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
        output = asyncio.run(
            list_tools(server_config=server_config, command=server_config["command"], args=processed_args, env=processed_env)
        )
        # Add processed parameters to output
        output_with_name = {
            "name": server_name,
            "config": input_configs,
            "command": server_config["command"],
            "args": [arg.replace("${input:", "${") if isinstance(arg, str) else arg for arg in server_config.get("args", [])],
            "env": [
                {"key": k, "value": v.replace("${input:", "${") if isinstance(v, str) else v}
                for k, v in server_config.get("env", {}).items()
            ],
            **output,
        }
        output_json = json.dumps(output_with_name, indent=2)
        click.echo(output_json)
        try:
            pyperclip.copy(output_json)
            click.secho("\nOutput copied to clipboard!", fg="green")
        except pyperclip.PyperclipException as e:
            click.secho(f"\nFailed to copy to clipboard: {e!s}", fg="yellow")


async def list_tools(server_config: dict, command: str, args: list[str], env: dict[str, str]):
    command_path = shutil.which(command)
    if not command_path:
        raise click.UsageError(f"Command not found: {command}")

    try:
        # Only support stdio server type
        server_type = server_config.get("type", "stdio")
        if server_type != "stdio":
            raise click.UsageError(f"Only stdio MCP servers are supported. Found type: {server_type}")

        server_params = StdioServerParameters(
            command=command_path,
            args=args,
            env=env,
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
    except Exception as e:
        raise click.UsageError("Could not connect to MCP server: " + str(e)) from e
