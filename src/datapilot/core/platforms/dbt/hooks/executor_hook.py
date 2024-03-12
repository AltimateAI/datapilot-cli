import argparse
import time
from pathlib import Path
from typing import Optional
from typing import Sequence

from datapilot.config.config import load_config
from datapilot.config.utils import get_insight_project_path
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.executor import DBTInsightGenerator
from datapilot.core.platforms.dbt.formatting import generate_model_insights_table
from datapilot.core.platforms.dbt.formatting import generate_project_insights_table
from datapilot.utils.formatting.utils import tabulate_data
from datapilot.utils.utils import generate_partial_manifest_catalog
from datapilot.utils.utils import get_tmp_dir_path


def main(argv: Optional[Sequence[str]] = None):
    start_time = time.time()
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--config-path",
        nargs="*",
        help="Path of the config file to be used for the insight generation",
    )

    args = parser.parse_known_args(argv)

    config = {}
    if hasattr(args[0], "config_path") and args[0].config_path:
        print(f"Using config file: {args[0].config_path[0]}")
        config = load_config(args[0].config_path[0])

    changed_files = args[1]

    tmp_folder = get_tmp_dir_path()
    manifest_path = Path(tmp_folder / "manifest.json")
    catalog_path = Path(tmp_folder / "catalog.json")
    base_path = get_insight_project_path(config)
    selected_models = generate_partial_manifest_catalog(
        changed_files, manifest_path=manifest_path, catalog_path=catalog_path, base_path=base_path
    )
    insight_generator = DBTInsightGenerator(
        manifest_path=manifest_path, catalog_path=catalog_path, config=config, selected_models=selected_models
    )
    reports = insight_generator.run()
    if reports:
        model_report = generate_model_insights_table(reports[MODEL])
        if len(model_report) > 0:
            print("--" * 50)
            print("Model Insights")
            print("--" * 50)
        for model_id, report in model_report.items():
            print(f"Model: {model_id}")
            print(f"File path: {report['path']}")
            print(tabulate_data(report["table"], headers="keys"))
            print("\n")

        project_report = generate_project_insights_table(reports[PROJECT])
        if len(project_report) > 0:
            print("--" * 50)
            print("Project Insights")
            print("--" * 50)
            print(tabulate_data(project_report, headers="keys"))

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time taken: {round(total_time, 2)} seconds")


if __name__ == "__main__":
    exit(main())
