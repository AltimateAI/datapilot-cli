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
        return
    
    try:
        config_data = json.loads(content)
        mcp_config = config_data.get("mcp", {})
        
        # Process inputs
        inputs = []
        for input_def in mcp_config.get("inputs", []):
            inputs.append(InputParameter(
                name=input_def.get("name", input_def["id"]),
                type=input_def["type"],
                required=input_def.get("required", False),
                key=input_def["id"].lower(),
                description=input_def["description"],
                encrypted=input_def.get("password", False)
            ))
        
        # Process servers
        tool_config = []
        for server_name, server_def in mcp_config.get("servers", {}).items():
            # Command config
            tool_config.append(IntegrationConfigItem(
                key="command",
                value=server_def["command"]
            ))
            
            # Arguments config
            tool_config.append(IntegrationConfigItem(
                key="arguments",
                value=server_def.get("args", [])
            ))
            
            # Environment variables
            env_items = []
            for var_name, var_value in server_def.get("env", {}).items():
                if var_value.startswith("${input:"):
                    _, input_id = var_value[2:-1].split(":")
                    var_value = f"${{{input_id}}}"
                
                env_items.append({"key": var_name, "value": var_value})
            
            tool_config.append(IntegrationConfigItem(
                key="env",
                value=env_items
            ))
        
        integration_model = CustomDatamateIntegrationModel(
            config=inputs,
            toolConfig=tool_config
        )
        
        click.echo(json.dumps(integration_model, cls=EnhancedJSONEncoder, indent=2))
    
    except Exception as e:
        click.echo(f"Error processing config: {str(e)}")

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

