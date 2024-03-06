import argparse
import time
from pathlib import Path
from typing import Optional
from typing import Sequence

from datapilot.config.config import load_config
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.executor import DBTInsightGenerator
from datapilot.utils.utils import generate_partial_manifest_catalog
from datapilot.utils.utils import get_tmp_dir_path


def executor_hook(argv: Optional[Sequence[str]] = None):
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
    generate_partial_manifest_catalog(
        changed_files,
        manifest_path=manifest_path,
        catalog_path=catalog_path,
    )
    insight_generator = DBTInsightGenerator(manifest_path=manifest_path, catalog_path=catalog_path, config=config)
    reports = insight_generator.run()
    if reports:
        for model, insights in reports[MODEL].items():
            print(model)
            print(insights)
            print("\n\n\n")
        for insight in reports[PROJECT]:
            print(PROJECT)
            print(insight)
            print("\n\n\n")

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time taken: {round(total_time, 2)} seconds")


if __name__ == "__main__":
    exit(executor_hook())
