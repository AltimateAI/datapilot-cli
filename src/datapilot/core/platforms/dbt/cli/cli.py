import logging

import click

from datapilot.clients.altimate.utils import check_token_and_instance
from datapilot.clients.altimate.utils import get_all_dbt_configs
from datapilot.clients.altimate.utils import onboard_file
from datapilot.clients.altimate.utils import start_dbt_ingestion
from datapilot.clients.altimate.utils import validate_credentials
from datapilot.clients.altimate.utils import validate_permissions
from datapilot.config.config import load_config
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.executor import DBTInsightGenerator
from datapilot.core.platforms.dbt.formatting import generate_model_insights_table
from datapilot.core.platforms.dbt.formatting import generate_project_insights_table
from datapilot.core.platforms.dbt.utils import load_catalog
from datapilot.core.platforms.dbt.utils import load_manifest
from datapilot.utils.formatting.utils import tabulate_data
from datapilot.utils.utils import map_url_to_instance

logging.basicConfig(level=logging.INFO)


# New dbt group
@click.group()
def dbt():
    """DBT specific commands."""


@dbt.command("project-health")
@click.option("--token", required=False, help="Your API token for authentication.")
@click.option("--instance-name", required=False, help="Your tenant ID.")
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
@click.option(
    "--config-name",
    required=False,
    help="Name of the DBT config to use from the API",
)
@click.option(
    "--select",
    required=False,
    default=None,
    help="Selective model testing. Specify one or more models to run tests on.",
)
@click.option("--backend-url", required=False, help="Altimate's Backend URL", default="https://api.myaltimate.com")
def project_health(
    token,
    instance_name,
    manifest_path,
    catalog_path,
    config_path=None,
    config_name=None,
    select=None,
    backend_url="https://api.myaltimate.com",
):
    """
    Validate the DBT project's configuration and structure.
    :param manifest_path: Path to the DBT manifest file.
    """
    config = None
    if config_path:
        config = load_config(config_path)
    elif config_name and token and instance_name:
        # Get configs from API
        configs = get_all_dbt_configs(token, instance_name, backend_url)
        if configs and "items" in configs:
            # Find config by name
            matching_configs = [c for c in configs["items"] if c["name"] == config_name]
            if matching_configs:
                # Get the config directly from the API response
                click.echo(f"Using config: {config_name}")
                config = matching_configs[0].get("config", {})
            else:
                click.echo(f"No config found with name: {config_name}")
                return
        else:
            click.echo("Failed to fetch configs from API")
            return

    selected_models = []
    if select:
        selected_models = select.split(" ")
    manifest = load_manifest(manifest_path)
    catalog = load_catalog(catalog_path) if catalog_path else None

    insight_generator = DBTInsightGenerator(
        manifest=manifest,
        catalog=catalog,
        config=config,
        selected_models=selected_models,
        token=token,
        instance_name=instance_name,
        backend_url=backend_url,
    )
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
@click.option("--token", prompt="API Token", help="Your API token for authentication.")
@click.option("--instance-name", prompt="Instance Name", help="Your tenant ID.")
@click.option("--dbt_core_integration_id", prompt="DBT Core Integration ID", help="DBT Core Integration ID")
@click.option(
    "--dbt_core_integration_environment", default="PROD", prompt="DBT Core Integration Environment", help="DBT Core Integration Environment"
)
@click.option("--manifest-path", required=True, prompt="Manifest Path", help="Path to the manifest file.")
@click.option("--catalog-path", required=False, prompt=False, help="Path to the catalog file.")
@click.option("--backend-url", required=False, help="Altimate's Backend URL", default="https://api.myaltimate.com")
def onboard(
    token,
    instance_name,
    dbt_core_integration_id,
    dbt_core_integration_environment,
    manifest_path,
    catalog_path,
    backend_url="https://api.myaltimate.com",
):
    """Onboard a manifest file to DBT."""
    check_token_and_instance(token, instance_name)

    if not validate_credentials(token, backend_url, instance_name):
        click.echo("Error: Invalid credentials.")
        return

    if not validate_permissions(token, backend_url, instance_name):
        click.echo("Error: You don't have permission to perform this action.")
        return

    # This will throw error if manifest file is incorrect
    try:
        load_manifest(manifest_path)
    except Exception as e:
        click.echo(f"Error: {e}")
        return

    response = onboard_file(
        token, instance_name, dbt_core_integration_id, dbt_core_integration_environment, "manifest", manifest_path, backend_url
    )
    if response["ok"]:
        click.echo("Manifest onboarded successfully!")
    else:
        click.echo(f"{response['message']}")

    if not catalog_path:
        return

    response = onboard_file(
        token, instance_name, dbt_core_integration_id, dbt_core_integration_environment, "catalog", catalog_path, backend_url
    )
    if response["ok"]:
        click.echo("Catalog onboarded successfully!")
    else:
        click.echo(f"{response['message']}")

    response = start_dbt_ingestion(token, instance_name, dbt_core_integration_id, dbt_core_integration_environment, backend_url)
    if response["ok"]:
        url = map_url_to_instance(backend_url, instance_name)
        if not url:
            click.echo("Manifest and catalog ingestion has started.")
        else:
            url = f"{url}/settings/integrations/{dbt_core_integration_id}/{dbt_core_integration_environment}"
            click.echo(f"Manifest and catalog ingestion has started. You can check the status at {url}")
    else:
        click.echo(f"{response['message']}")
