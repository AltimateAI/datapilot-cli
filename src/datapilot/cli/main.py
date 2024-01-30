import click

from datapilot.core.platforms.dbt.cli.cli import dbt


@click.group()
def datapilot():
    """Altimate CLI for DBT project management."""


datapilot.add_command(dbt)
