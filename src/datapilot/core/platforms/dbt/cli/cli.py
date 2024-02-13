import logging

import click

from datapilot.clients.altimate.client import APIClient
from datapilot.clients.altimate.utils import check_token_and_instance
from datapilot.clients.altimate.utils import onboard_manifest
from datapilot.clients.altimate.utils import validate_credentials
from datapilot.config.config import load_config
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.executor import DBTInsightGenerator
from datapilot.core.platforms.dbt.formatting import generate_model_insights_table
from datapilot.core.platforms.dbt.formatting import generate_project_insights_table
from datapilot.core.platforms.dbt.utils import load_manifest
from datapilot.core.platforms.sql.executor import DBTSqlInsightGenerator
from datapilot.utils.formatting.utils import tabulate_data

logging.basicConfig(level=logging.INFO)


# New dbt group
@click.group()
def dbt():
    """DBT specific commands."""


@dbt.command("project-health")
@click.option(
    "--manifest-path",
    required=True,
    help="Path to the DBT manifest file",
)
@click.option(
    "--catalog-path",
    required=False,
    help="Path to the DBT catalog file",
)
@click.option(
    "--config-path",
    required=False,
    help="Path to the DBT config file",
)
def project_health(manifest_path, catalog_path, config_path=None):
    """
    Validate the DBT project's configuration and structure.
    :param manifest_path: Path to the DBT manifest file.
    """
    config = None
    if config_path:
        config = load_config(config_path)
    insight_generator = DBTInsightGenerator(manifest_path, catalog_path=catalog_path, config=config)
    reports = insight_generator.run()

    package_insights = reports[PROJECT]
    model_insights = reports[MODEL]
    model_report = generate_model_insights_table(model_insights)
    if len(model_report) > 0:
        click.echo("--" * 50)
        click.echo("Model Insights")
        click.echo("--" * 50)
    for model_id, report in model_report.items():
        click.echo(f"Model: {model_id}")
        click.echo(f"File path: {report['path']}")
        click.echo(tabulate_data(report["table"], headers="keys"))
        click.echo("\n")

    if len(package_insights) > 0:
        project_report = generate_project_insights_table(package_insights)
        click.echo("--" * 50)
        click.echo("Project Insights")
        click.echo("--" * 50)
        click.echo(tabulate_data(project_report, headers="keys"))


@dbt.command("onboard")
@click.option("--token", required=False, help="Your API token for authentication.")
@click.option("--instance-name", required=False, help="Instance Name")
@click.option("--dbt_core_integration_id", required=True, help="DBT Core Integration ID")
@click.option("--manifest-path", required=True, prompt="Manifest Path", help="Path to the manifest file.")
@click.option("--backend-url", required=False, default="https://api.myaltimate.com", help="Altimate's Backend URL")
def onboard(token, instance_name, dbt_core_integration_id, manifest_path, backend_url="https://api.myaltimate.com", env=None):
    """Onboard a manifest file to DBT."""
    check_token_and_instance(token, instance_name)

    if not validate_credentials(token, backend_url, instance_name):
        click.echo("Error: Invalid credentials.")
        return

    # This will throw error if manifest file is incorrect
    load_manifest(manifest_path)

    response = onboard_manifest(token, instance_name, dbt_core_integration_id, manifest_path, backend_url)

    if response["ok"]:
        click.echo("Manifest onboarded successfully!")
    else:
        click.echo(f"{response['message']}")


@click.group()
def dbt():
    """DBT specific commands."""


@dbt.command("sql-insights")
@click.option("--adapter", required=True, help="The adapter to use for the DBT project.")
@click.option("--manifest-path", required=True, help="Path to the DBT manifest file")
@click.option("--catalog-path", required=False, help="Path to the DBT catalog file")
@click.option("--config-path", required=False, help="Path to the DBT config file")
@click.option("--token", help="Your API token for authentication.")
@click.option("--instance-name", help="Your tenant ID.")
@click.option("--backend-url", required=False, help="Altimate's Backend URL", default="https://api.myaltimate.com")
def sql_insights(
    adapter, manifest_path, catalog_path, config_path=None, token=None, instance_name=None, backend_url="https://api.myaltimate.com"
):
    """
    Validate the DBT project's configuration and structure.
    :param manifest_path: Path to the DBT manifest file.
    """
    config = None
    if config_path:
        config = load_config(config_path)

    check_token_and_instance(token, instance_name)

    if not validate_credentials(token, backend_url, instance_name):
        click.echo("Error: Invalid credentials.")
        return

    api_client = APIClient(api_token=token, base_url=backend_url, tenant=instance_name)

    insight_generator = DBTSqlInsightGenerator(
        manifest_path=manifest_path, catalog_path=catalog_path, adapter=adapter, config=config, api_client=api_client
    )
    reports = insight_generator.run()
    model_report = generate_model_insights_table(reports)
    if len(model_report) > 0:
        click.echo("--" * 50)
        click.echo("Model Insights")
        click.echo("--" * 50)
    for model_id, report in model_report.items():
        click.echo(f"Model: {model_id}")
        click.echo(f"File path: {report['path']}")
        click.echo(tabulate_data(report["table"], headers="keys"))
        click.echo("\n")
