import json
import os
import re
from pathlib import Path

import click
from dotenv import load_dotenv

from datapilot.core.mcp_utils.mcp import mcp
from datapilot.core.platforms.dbt.cli.cli import dbt


def load_config_from_file():
    """Load configuration from ~/.altimate/altimate.json if it exists."""
    config_path = Path.home() / ".altimate" / "altimate.json"

    if not config_path.exists():
        return {}

    try:
        with open(config_path) as f:
            config = json.load(f)
        return config
    except (OSError, json.JSONDecodeError) as e:
        click.echo(f"Warning: Failed to load config from {config_path}: {e}", err=True)
        return {}


def substitute_env_vars(value):
    """Replace ${env:ENV_VARIABLE} patterns with actual environment variable values."""
    if not isinstance(value, str):
        return value

    # Pattern to match ${env:VARIABLE_NAME}
    pattern = r"\$\{env:([^}]+)\}"

    def replacer(match):
        env_var = match.group(1)
        return os.environ.get(env_var, match.group(0))

    return re.sub(pattern, replacer, value)


def process_config(config):
    """Process configuration dictionary to substitute environment variables."""
    processed = {}
    for key, value in config.items():
        processed[key] = substitute_env_vars(value)
    return processed


@click.group()
@click.option("--token", required=False, help="Your API token for authentication.", hide_input=True)
@click.option("--instance-name", required=False, help="Your tenant ID.")
@click.option("--backend-url", required=False, help="Altimate's Backend URL", default="https://api.myaltimate.com")
@click.pass_context
def datapilot(ctx, token, instance_name, backend_url):
    """Altimate CLI for DBT project management."""
    # Load .env file from current directory if it exists
    load_dotenv()

    # Load configuration from file
    file_config = load_config_from_file()
    file_config = process_config(file_config)

    # Map config file keys to CLI option names
    config_mapping = {"altimateApiKey": "token", "altimateInstanceName": "instance_name", "altimateUrl": "backend_url"}

    # Store common options in context, with CLI args taking precedence
    ctx.ensure_object(dict)

    # Apply file config first
    for file_key, cli_key in config_mapping.items():
        if file_key in file_config:
            ctx.obj[cli_key] = file_config[file_key]

    # Override with CLI arguments if provided
    if token is not None:
        ctx.obj["token"] = token
    if instance_name is not None:
        ctx.obj["instance_name"] = instance_name
    if backend_url != "https://api.myaltimate.com":  # Only override if not default
        ctx.obj["backend_url"] = backend_url

    # Set defaults if nothing was provided
    ctx.obj.setdefault("token", None)
    ctx.obj.setdefault("instance_name", None)
    ctx.obj.setdefault("backend_url", "https://api.myaltimate.com")


datapilot.add_command(dbt)
datapilot.add_command(mcp)
