import click

from datapilot.core.platforms.dbt.cli import dbt


@click.group()
def datapilot():
    """Altimate CLI for DBT project management."""
    pass


datapilot.add_command(dbt)