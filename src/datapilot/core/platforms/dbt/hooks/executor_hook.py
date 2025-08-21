import argparse
import sys
import time
from pathlib import Path
from typing import Optional
from typing import Sequence
from typing import Tuple

from datapilot.clients.altimate.utils import get_all_dbt_configs
from datapilot.config.config import load_config
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.executor import DBTInsightGenerator
from datapilot.core.platforms.dbt.formatting import generate_model_insights_table
from datapilot.core.platforms.dbt.formatting import generate_project_insights_table
from datapilot.core.platforms.dbt.utils import load_catalog
from datapilot.core.platforms.dbt.utils import load_manifest
from datapilot.utils.formatting.utils import tabulate_data


def get_config(name: str, matching_configs: list) -> dict:
    """Extract config from matching configs by name."""
    if matching_configs:
        print(f"Using config from API: {name} Config ID: {matching_configs[0]['id']}", file=sys.stderr)
        return matching_configs[0].get("config", {})
    else:
        print(f"No config found with name: {name}", file=sys.stderr)
        print("Pre-commit hook failed: Config not found.", file=sys.stderr)
        sys.exit(1)


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


def setup_argument_parser() -> argparse.ArgumentParser:
    """Set up and return the argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", nargs="*", help="Path of the config file to be used for the insight generation")
    parser.add_argument("--base-path", nargs="*", help="Base path of the dbt project")
    parser.add_argument("--token", help="Your API token for authentication.")
    parser.add_argument("--instance-name", help="Your tenant ID.")
    parser.add_argument("--backend-url", help="Altimate's Backend URL", default="https://api.myaltimate.com")
    parser.add_argument("--config-name", help="Name of the DBT config to use from the API")
    parser.add_argument("--manifest-path", help="Path to the DBT manifest file (defaults to ./target/manifest.json)")
    parser.add_argument("--catalog-path", help="Path to the DBT catalog file (defaults to ./target/catalog.json)")
    return parser


def extract_arguments(args) -> Tuple[str, str, str, str, str, str, str, str]:
    """Extract and return common arguments from parsed args."""
    config_name = getattr(args, "config_name", None)
    token = getattr(args, "token", None)
    instance_name = getattr(args, "instance_name", None)
    backend_url = getattr(args, "backend_url", "https://api.myaltimate.com")

    # Extract config_path and base_path (they are lists)
    config_path = args.config_path[0] if args.config_path else None
    base_path = args.base_path[0] if args.base_path else "./"

    # Extract manifest and catalog paths
    manifest_path = getattr(args, "manifest_path", None)
    catalog_path = getattr(args, "catalog_path", None)

    return config_name, token, instance_name, backend_url, config_path, base_path, manifest_path, catalog_path


def load_config_from_file(config_path: str) -> dict:
    """Load configuration from a file."""
    print(f"Loading config from file: {config_path}", file=sys.stderr)
    if not validate_config_file(config_path):
        print("Pre-commit hook failed: Invalid config file.", file=sys.stderr)
        sys.exit(1)
    config = load_config(config_path)
    print("Config loaded successfully from file", file=sys.stderr)
    return config


def load_config_from_api(config_name: str, token: str, instance_name: str, backend_url: str) -> dict:
    """Load configuration from API."""
    print(f"Fetching config '{config_name}' from API...", file=sys.stderr)
    try:
        configs = get_all_dbt_configs(token, instance_name, backend_url)
        if configs and "items" in configs:
            matching_configs = [c for c in configs["items"] if c["name"] == config_name]
            return get_config(config_name, matching_configs)
        else:
            print("Failed to fetch configs from API", file=sys.stderr)
            print("Pre-commit hook failed: Unable to fetch configs.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error fetching config from API: {e}", file=sys.stderr)
        print("Pre-commit hook failed: API error.", file=sys.stderr)
        sys.exit(1)


def get_configuration(config_name: str, token: str, instance_name: str, backend_url: str, config_path: str) -> dict:
    """Get configuration from file, API, or use defaults."""
    if config_path:
        return load_config_from_file(config_path)
    elif config_name and token and instance_name:
        return load_config_from_api(config_name, token, instance_name, backend_url)
    else:
        print("No config provided. Using default configuration.", file=sys.stderr)
        return {}


def get_file_paths(base_path: str, manifest_path: str, catalog_path: str) -> Tuple[str, str]:
    """Determine manifest and catalog file paths."""
    if not manifest_path:
        manifest_path = str(Path(base_path) / "target" / "manifest.json")
        print(f"Using default manifest path: {manifest_path}", file=sys.stderr)
    else:
        print(f"Using provided manifest path: {manifest_path}", file=sys.stderr)

    if not catalog_path:
        catalog_path = str(Path(base_path) / "target" / "catalog.json")
        print(f"Using default catalog path: {catalog_path}", file=sys.stderr)
    else:
        print(f"Using provided catalog path: {catalog_path}", file=sys.stderr)

    return manifest_path, catalog_path


def load_manifest_file(manifest_path: str):
    """Load and validate manifest file."""
    print("Loading manifest file...", file=sys.stderr)
    try:
        manifest = load_manifest(manifest_path)
        node_count = get_node_count(manifest)
        print(f"Manifest loaded successfully with {node_count} nodes", file=sys.stderr)
        return manifest
    except Exception as e:
        print(f"Error loading manifest from {manifest_path}: {e}", file=sys.stderr)
        print("Pre-commit hook failed: Unable to load manifest file.", file=sys.stderr)
        sys.exit(1)


def load_catalog_file(catalog_path: str):
    """Load catalog file if it exists."""
    if not Path(catalog_path).exists():
        print(f"Catalog file not found at {catalog_path}, continuing without catalog", file=sys.stderr)
        return None

    print("Loading catalog file...", file=sys.stderr)
    try:
        catalog = load_catalog(catalog_path)
        node_count = get_node_count(catalog)
        print(f"Catalog loaded successfully with {node_count} nodes", file=sys.stderr)
        return catalog
    except Exception as e:
        print(f"Warning: Error loading catalog from {catalog_path}: {e}", file=sys.stderr)
        print("Continuing without catalog...", file=sys.stderr)
        return None


def get_node_count(obj) -> int:
    """Get the number of nodes from manifest or catalog object."""
    if hasattr(obj, "nodes"):
        return len(obj.nodes)
    elif hasattr(obj, "get") and callable(obj.get):
        return len(obj.get("nodes", {}))
    else:
        return 0


def process_changed_files(changed_files: list) -> list:
    """Process changed files for selective model testing."""
    if changed_files:
        print(f"Processing {len(changed_files)} changed files for selective testing...", file=sys.stderr)
        print(f"Changed files: {', '.join(changed_files)}", file=sys.stderr)
        return changed_files
    else:
        print("No changed files detected. Running checks on all models.", file=sys.stderr)
        return []


def run_insight_generation(manifest, catalog, config, selected_models, token, instance_name, backend_url):
    """Run the insight generation process."""
    print("Initializing DBT Insight Generator...", file=sys.stderr)
    insight_generator = DBTInsightGenerator(
        manifest=manifest,
        catalog=catalog,
        config=config,
        selected_models=selected_models,
        token=token,
        instance_name=instance_name,
        backend_url=backend_url,
    )

    print("Running insight generation...", file=sys.stderr)
    return insight_generator.run()


def print_model_insights(model_report: dict):
    """Print model insights in a formatted table."""
    print("--" * 50, file=sys.stderr)
    print("Model Insights", file=sys.stderr)
    print("--" * 50, file=sys.stderr)
    for model_id, report in model_report.items():
        print(f"Model: {model_id}", file=sys.stderr)
        print(f"File path: {report['path']}", file=sys.stderr)
        print(tabulate_data(report["table"], headers="keys"), file=sys.stderr)
        print("\n", file=sys.stderr)


def print_project_insights(project_report: dict):
    """Print project insights in a formatted table."""
    print("--" * 50, file=sys.stderr)
    print("Project Insights", file=sys.stderr)
    print("--" * 50, file=sys.stderr)
    print(tabulate_data(project_report, headers="keys"), file=sys.stderr)


def process_reports(reports: dict) -> bool:
    """Process and display reports, return True if issues found."""
    if not reports:
        print("No insights generated. All checks passed!", file=sys.stderr)
        return False

    print("Insights generated successfully. Analyzing results...", file=sys.stderr)
    model_report = generate_model_insights_table(reports[MODEL])
    project_report = generate_project_insights_table(reports[PROJECT])

    has_model_issues = len(model_report) > 0
    has_project_issues = len(project_report) > 0

    if has_model_issues:
        print_model_insights(model_report)

    if has_project_issues:
        print_project_insights(project_report)

    if has_model_issues or has_project_issues:
        print("\nPre-commit hook failed: DataPilot found issues that need to be addressed.", file=sys.stderr)
        return True
    else:
        print("All checks passed! No issues found.", file=sys.stderr)
        return False


def log_warnings(token: str, instance_name: str):
    """Log warnings for missing authentication parameters."""
    if not token:
        print("Warning: No API token provided. Using default configuration.", file=sys.stderr)
        print("To specify a token, use: --token 'your-token'", file=sys.stderr)

    if not instance_name:
        print("Warning: No instance name provided. Using default configuration.", file=sys.stderr)
        print("To specify an instance, use: --instance-name 'your-instance'", file=sys.stderr)


def main(argv: Optional[Sequence[str]] = None):
    start_time = time.time()
    print("Starting DataPilot pre-commit hook...", file=sys.stderr)

    # Parse arguments
    parser = setup_argument_parser()
    args = parser.parse_known_args(argv)

    # Extract arguments
    config_name, token, instance_name, backend_url, config_path, base_path, manifest_path, catalog_path = extract_arguments(args[0])

    # Get configuration
    config = get_configuration(config_name, token, instance_name, backend_url, config_path)

    # Log warnings for missing auth parameters
    log_warnings(token, instance_name)

    # Determine file paths
    manifest_path, catalog_path = get_file_paths(base_path, manifest_path, catalog_path)

    # Load manifest and catalog
    manifest = load_manifest_file(manifest_path)
    catalog = load_catalog_file(catalog_path)

    # Process changed files
    selected_models = process_changed_files(args[1])

    try:
        # Run insight generation
        reports = run_insight_generation(manifest, catalog, config, selected_models, token, instance_name, backend_url)

        # Process results
        has_issues = process_reports(reports)
        if has_issues:
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
