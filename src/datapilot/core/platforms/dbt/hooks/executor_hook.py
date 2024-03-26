import argparse
import time
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

    args = parser.parse_known_args(argv)
    # print(f"args: {args}", file=sys.__stdout__)
    config = {}
    if hasattr(args[0], "config_path") and args[0].config_path:
        # print(f"Using config file: {args[0].config_path[0]}")
        config = load_config(args[0].config_path[0])

    base_path = "./"
    if hasattr(args[0], "base_path") and args[0].base_path:
        base_path = args[0].base_path[0]

    changed_files = args[1]
    # print(f"Changed files: {changed_files}")

    if not changed_files:
        # print("No changed files detected - test. Exiting...")
        return

    # print(f"Changed files: {changed_files}", file=sys.__stdout__)
    selected_models, manifest, catalog = generate_partial_manifest_catalog(changed_files, base_path=base_path)
    # print("se1ected models", selected_models, file=sys.__stdout__)
    insight_generator = DBTInsightGenerator(
        manifest=manifest,
        catalog=catalog,
        config=config,
        selected_model_ids=selected_models,
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

        exit(1)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time taken: {round(total_time, 2)} seconds")


if __name__ == "__main__":
    exit(main())
