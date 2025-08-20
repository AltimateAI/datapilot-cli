import argparse
import sys
import time
from pathlib import Path
from typing import Optional
from typing import Sequence

from datapilot.clients.altimate.utils import get_all_dbt_configs
from datapilot.config.config import load_config
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.executor import DBTInsightGenerator
from datapilot.core.platforms.dbt.formatting import generate_model_insights_table
from datapilot.core.platforms.dbt.formatting import generate_project_insights_table
from datapilot.utils.formatting.utils import tabulate_data
from datapilot.utils.utils import generate_partial_manifest_catalog


def validate_config_file(config_path: str) -> bool:
    """Validate that the config file exists and is not empty."""
    if not Path(config_path).exists():
        print(f"Error: Config file '{config_path}' does not exist.", file=sys.stderr)
        return False

    try:
        config = load_config(config_path)
        if not config:
            print(f"Error: Config file '{config_path}' is empty or invalid.", file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"Error: Failed to load config file '{config_path}': {e}", file=sys.stderr)
        return False


def main(argv: Optional[Sequence[str]] = None):
    start_time = time.time()
    print("Starting DataPilot pre-commit hook...", file=sys.stderr)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config-path",
        nargs="*",
        help="Path of the config file to be used for the insight generation",
    )

    parser.add_argument(
        "--base-path",
        nargs="*",
        help="Base path of the dbt project",
    )

    parser.add_argument(
        "--token",
        help="Your API token for authentication.",
    )

    parser.add_argument(
        "--instance-name",
        help="Your tenant ID.",
    )

    parser.add_argument("--backend-url", help="Altimate's Backend URL", default="https://api.myaltimate.com")

    parser.add_argument(
        "--config-name",
        help="Name of the DBT config to use from the API",
    )

    args = parser.parse_known_args(argv)

    # Handle config loading like in project_health command
    config = None
    if hasattr(args[0], "config_path") and args[0].config_path:
        config_path = args[0].config_path[0]
        print(f"Loading config from file: {config_path}", file=sys.stderr)
        if not validate_config_file(config_path):
            print("Pre-commit hook failed: Invalid config file.", file=sys.stderr)
            sys.exit(1)
        config = load_config(config_path)
        print("Config loaded successfully from file", file=sys.stderr)
    elif (
        hasattr(args[0], "config_name")
        and args[0].config_name
        and hasattr(args[0], "token")
        and args[0].token
        and hasattr(args[0], "instance_name")
        and args[0].instance_name
    ):
        config_name = args[0].config_name
        token = args[0].token
        instance_name = args[0].instance_name
        backend_url = getattr(args[0], "backend_url", "https://api.myaltimate.com")

        print(f"Fetching config '{config_name}' from API...", file=sys.stderr)
        try:
            # Get configs from API
            configs = get_all_dbt_configs(token, instance_name, backend_url)
            if configs and "items" in configs:
                # Find config by name
                matching_configs = [c for c in configs["items"] if c["name"] == config_name]
                if matching_configs:
                    # Get the config directly from the API response
                    print(f"Using config from API: {config_name}", file=sys.stderr)
                    config = matching_configs[0].get("config", {})
                else:
                    print(f"No config found with name: {config_name}", file=sys.stderr)
                    print("Pre-commit hook failed: Config not found.", file=sys.stderr)
                    sys.exit(1)
            else:
                print("Failed to fetch configs from API", file=sys.stderr)
                print("Pre-commit hook failed: Unable to fetch configs.", file=sys.stderr)
                sys.exit(1)
        except Exception as e:
            print(f"Error fetching config from API: {e}", file=sys.stderr)
            print("Pre-commit hook failed: API error.", file=sys.stderr)
            sys.exit(1)
    else:
        print("No config provided. Using default configuration.", file=sys.stderr)
        config = {}

    base_path = "./"
    if hasattr(args[0], "base_path") and args[0].base_path:
        base_path = args[0].base_path[0]
        print(f"Using base path: {base_path}", file=sys.stderr)

    # Get authentication parameters
    token = getattr(args[0], "token", None)
    instance_name = getattr(args[0], "instance_name", None)
    backend_url = getattr(args[0], "backend_url", "https://api.myaltimate.com")

    # Validate authentication parameters
    if not token:
        print("Warning: No API token provided. Using default configuration.", file=sys.stderr)
        print("To specify a token, use: --token 'your-token'", file=sys.stderr)

    if not instance_name:
        print("Warning: No instance name provided. Using default configuration.", file=sys.stderr)
        print("To specify an instance, use: --instance-name 'your-instance'", file=sys.stderr)

    changed_files = args[1]

    if not changed_files:
        print("No changed files detected. Skipping datapilot checks.", file=sys.stderr)
        return

    print(f"Processing {len(changed_files)} changed files...", file=sys.stderr)
    print(f"Changed files: {', '.join(changed_files)}", file=sys.stderr)

    try:
        print("Generating partial manifest and catalog from changed files...", file=sys.stderr)
        selected_models, manifest, catalog = generate_partial_manifest_catalog(changed_files, base_path=base_path)
        print(f"Generated manifest with {len(manifest.get('nodes', {}))} nodes", file=sys.stderr)
        if catalog:
            print(f"Generated catalog with {len(catalog.get('nodes', {}))} nodes", file=sys.stderr)
        else:
            print("No catalog generated (catalog file not available)", file=sys.stderr)

        print("Initializing DBT Insight Generator...", file=sys.stderr)
        insight_generator = DBTInsightGenerator(
            manifest=manifest,
            catalog=catalog,
            config=config,
            selected_model_ids=selected_models,
            token=token,
            instance_name=instance_name,
            backend_url=backend_url,
        )

        print("Running insight generation...", file=sys.stderr)
        reports = insight_generator.run()

        if reports:
            print("Insights generated successfully. Analyzing results...", file=sys.stderr)
            model_report = generate_model_insights_table(reports[MODEL])
            if len(model_report) > 0:
                print("--" * 50, file=sys.stderr)
                print("Model Insights", file=sys.stderr)
                print("--" * 50, file=sys.stderr)
            for model_id, report in model_report.items():
                print(f"Model: {model_id}", file=sys.stderr)
                print(f"File path: {report['path']}", file=sys.stderr)
                print(tabulate_data(report["table"], headers="keys"), file=sys.stderr)
                print("\n", file=sys.stderr)

            project_report = generate_project_insights_table(reports[PROJECT])
            if len(project_report) > 0:
                print("--" * 50, file=sys.stderr)
                print("Project Insights", file=sys.stderr)
                print("--" * 50, file=sys.stderr)
                print(tabulate_data(project_report, headers="keys"), file=sys.stderr)

            print("\nPre-commit hook failed: DataPilot found issues that need to be addressed.", file=sys.stderr)
            sys.exit(1)
        else:
            print("No insights generated. All checks passed!", file=sys.stderr)

    except Exception as e:
        print(f"Error running DataPilot checks: {e}", file=sys.stderr)
        print("Pre-commit hook failed due to an error.", file=sys.stderr)
        sys.exit(1)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"DataPilot checks completed successfully in {round(total_time, 2)} seconds", file=sys.stderr)


if __name__ == "__main__":
    exit(main())
