import os

import click

from datapilot.core.platforms.dbt.cli import dbt
from datapilot.core.platforms.dbt.utils import load_manifest
from datapilot.utils.utils import onboard_manifest


@click.group()
def datapilot():
    """Altimate CLI for DBT project management."""
    pass


@dbt.command()
@click.option("--api-token", prompt="API Token", help="Your API token for authentication.")
@click.option("--tenant", prompt="Tenant", help="Your tenant ID.")
@click.option("--dbt_core_integration_id", prompt="DBT Core Integration ID", help="DBT Core Integration ID")
@click.option("--manifest-path", prompt="Manifest Path", help="Path to the manifest file.")
def onboard(token, tenant, dbt_core_integration_id, manifest_path, env):
    """Onboard a manifest file to DBT."""
    if not token and env:
        token = os.environ.get("DBT_API_TOKEN")

    if not token or not tenant:
        print("Error: API Token is required.")
        return

    # if not validate_credentials(token, tenant):
    #     print("Error: Invalid credentials.")
    #     return

    # This will throw error if manifest file is incorrect
    load_manifest(manifest_path)

    onboard_manifest(token, tenant, dbt_core_integration_id, manifest_path)


datapilot.add_command(dbt)
