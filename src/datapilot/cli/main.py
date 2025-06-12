import click

from datapilot import __version__
from datapilot.core.knowledge.cli import cli as knowledge
from datapilot.core.mcp_utils.mcp import mcp
from datapilot.core.platforms.dbt.cli.cli import dbt


@click.group()
@click.version_option(version=__version__, prog_name="datapilot")
def datapilot():
    """Altimate CLI for DBT project management."""


datapilot.add_command(dbt)
datapilot.add_command(mcp)
datapilot.add_command(knowledge)
