import json
import os
import re
from functools import wraps
from pathlib import Path
from typing import Dict
from typing import Optional

import click
from dotenv import load_dotenv


def load_config_from_file() -> Optional[Dict]:
    """Load configuration from ~/.altimate/altimate.json if it exists."""
    config_path = Path.home() / ".altimate" / "altimate.json"

    if not config_path.exists():
        return None

    try:
        with config_path.open() as f:
            config = json.load(f)
        return config
    except (OSError, json.JSONDecodeError) as e:
        click.echo(f"Warning: Failed to load config from {config_path}: {e}", err=True)
        return None


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

        final_token = token
        final_instance_name = instance_name
        final_backend_url = backend_url

        if final_token is None and final_instance_name is None:
            # Try to Load configuration from file if no CLI arguments are provided
            file_config = load_config_from_file()
            if file_config is not None:
                # File config is provided
                file_config = process_config(file_config)
                if "altimateApiKey" in file_config:
                    final_token = file_config["altimateApiKey"]
                if "altimateInstanceName" in file_config:
                    final_instance_name = file_config["altimateInstanceName"]
                if "altimateUrl" in file_config:
                    final_backend_url = file_config["altimateUrl"] or final_backend_url

        return f(final_token, final_instance_name, final_backend_url, *args, **kwargs)

    return wrapper
