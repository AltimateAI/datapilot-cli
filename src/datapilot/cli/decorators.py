import json
import os
import re
from functools import wraps
from pathlib import Path

import click
from dotenv import load_dotenv


def load_config_from_file():
    """Load configuration from ~/.altimate/altimate.json if it exists."""
    config_path = Path.home() / ".altimate" / "altimate.json"

    if not config_path.exists():
        return {}

    try:
        with config_path.open() as f:
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


def auth_options(f):
    """Decorator to add authentication options to commands."""

    @click.option("--token", required=False, help="Your API token for authentication.", hide_input=True)
    @click.option("--instance-name", required=False, help="Your tenant ID.")
    @click.option("--backend-url", required=False, help="Altimate's Backend URL", default="https://api.myaltimate.com")
    @wraps(f)
    def wrapper(token, instance_name, backend_url, *args, **kwargs):
        # Load .env file from current directory if it exists
        load_dotenv()

        # Load configuration from file
        file_config = load_config_from_file()
        file_config = process_config(file_config)

        # Map config file keys to CLI option names
        config_mapping = {"altimateApiKey": "token", "altimateInstanceName": "instance_name", "altimateUrl": "backend_url"}

        # Apply file config first, then override with CLI arguments if provided
        final_token = token
        final_instance_name = instance_name
        final_backend_url = backend_url

        # Use file config if CLI argument not provided
        if final_token is None and "altimateApiKey" in file_config:
            final_token = file_config["altimateApiKey"]
        if final_instance_name is None and "altimateInstanceName" in file_config:
            final_instance_name = file_config["altimateInstanceName"]
        if final_backend_url == "https://api.myaltimate.com" and "altimateUrl" in file_config:
            final_backend_url = file_config["altimateUrl"]

        # Set defaults if nothing was provided
        if final_token is None:
            final_token = None
        if final_instance_name is None:
            final_instance_name = None
        if final_backend_url is None:
            final_backend_url = "https://api.myaltimate.com"

        return f(final_token, final_instance_name, final_backend_url, *args, **kwargs)

    return wrapper
