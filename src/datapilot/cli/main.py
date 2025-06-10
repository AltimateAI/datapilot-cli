import click

from datapilot.core.mcp_utils.mcp import mcp
from datapilot.core.platforms.dbt.cli.cli import dbt


@click.group()
@click.option("--token", required=False, help="Your API token for authentication.")
@click.option("--instance-name", required=False, help="Your tenant ID.")
@click.option("--backend-url", required=False, help="Altimate's Backend URL", default="https://api.myaltimate.com")
@click.pass_context
def datapilot(ctx, token, instance_name, backend_url):
    """Altimate CLI for DBT project management."""
    # Store common options in context
    ctx.ensure_object(dict)
    ctx.obj['token'] = token
    ctx.obj['instance_name'] = instance_name
    ctx.obj['backend_url'] = backend_url


datapilot.add_command(dbt)
datapilot.add_command(mcp)
