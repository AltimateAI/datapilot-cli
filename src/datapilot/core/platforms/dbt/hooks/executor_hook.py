import argparse
import sys
import time
from pathlib import Path
from typing import Optional
from typing import Sequence

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

    # Validate config file if provided
    config = {}
    if hasattr(args[0], "config_path") and args[0].config_path:
        config_path = args[0].config_path[0]
        if not validate_config_file(config_path):
            print("Pre-commit hook failed: Invalid config file.", file=sys.stderr)
            sys.exit(1)
        config = load_config(config_path)

    base_path = "./"
    if hasattr(args[0], "base_path") and args[0].base_path:
        base_path = args[0].base_path[0]

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

    try:
        selected_models, manifest, catalog = generate_partial_manifest_catalog(changed_files, base_path=base_path)

        insight_generator = DBTInsightGenerator(
            manifest=manifest,
            catalog=catalog,
            config=config,
            selected_model_ids=selected_models,
            token=token,
            instance_name=instance_name,
            backend_url=backend_url,
        )

        reports = insight_generator.run()

        if reports:
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

    except Exception as e:
        print(f"Error running DataPilot checks: {e}", file=sys.stderr)
        print("Pre-commit hook failed due to an error.", file=sys.stderr)
        sys.exit(1)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"DataPilot checks completed successfully in {round(total_time, 2)} seconds", file=sys.stderr)


if __name__ == "__main__":
    exit(main())
