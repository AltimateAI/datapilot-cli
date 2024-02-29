import argparse
import logging

# from src.utils.formatting.utils import generate_model_insights_table
from typing import Dict
from typing import Optional
from typing import Sequence

from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.exceptions import AltimateCLIArgumentError
from datapilot.core.platforms.dbt.factory import DBTFactory
from datapilot.core.platforms.dbt.insights import INSIGHTS
from datapilot.core.platforms.dbt.utils import get_models
from datapilot.core.platforms.dbt.utils import load_catalog
from datapilot.core.platforms.dbt.utils import load_manifest
from datapilot.utils.formatting.utils import RED
from datapilot.utils.formatting.utils import YELLOW
from datapilot.utils.formatting.utils import color_text


class DBTInsightGenerator:
    def __init__(
        self,
        manifest_path: str,
        catalog_path: Optional[str] = None,
        run_results_path: Optional[str] = None,
        env: Optional[str] = None,
        config: Optional[Dict] = None,
        target: str = "dev",
        selected_models: Optional[str] = None,
    ):
        self.manifest_path = manifest_path
        self.catalog_path = catalog_path
        self.run_results_path = run_results_path
        self.target = target
        self.env = env
        self.config = config or {}
        manifest = load_manifest(self.manifest_path)

        self.manifest_wrapper = DBTFactory.get_manifest_wrapper(manifest)
        self.manifest_present = True
        self.catalog_present = False
        self.catalog_wrapper = None

        if catalog_path:
            catalog = load_catalog(self.catalog_path)
            self.catalog_wrapper = DBTFactory.get_catalog_wrapper(catalog)
            self.catalog_present = True

        self.run_results_present = False
        self.logger = logging.getLogger("dbt-insight-generator")

        self.nodes = self.manifest_wrapper.get_nodes()
        self.macros = self.manifest_wrapper.get_macros()
        self.sources = self.manifest_wrapper.get_sources()
        self.exposures = self.manifest_wrapper.get_exposures()
        self.children_map = self.manifest_wrapper.parent_to_child_map(self.nodes)
        self.tests = self.manifest_wrapper.get_tests()
        self.project_name = self.manifest_wrapper.get_package()
        self.selected_models = None
        self.selected_models_flag = False
        entities = {
            "nodes": self.nodes,
            "sources": self.sources,
            "exposures": self.exposures,
            "tests": self.tests,
        }
        if selected_models:
            self.selected_models_flag = True
            self.selected_models = get_models(
                selected_models,
                entities=entities,
            )
            if not self.selected_models:
                raise AltimateCLIArgumentError(
                    f"Invalid values provided in the --select argument. Could not find models associated with pattern: --select {' '.join(selected_models)}"
                )
        self.excluded_models = None
        self.excluded_models_flag = False

    def _check_if_skipped(self, insight):
        if self.config.get("disabled_insights", False):
            if insight.ALIAS in self.config.get("disabled_insights", []):
                return True
        return False

    def _check_if_model_skipped(self, insight):
        if self.selected_models:
            if insight.ALIAS not in self.selected_models:
                return True
        return False

    def run(self):
        reports = {
            MODEL: {},
            PROJECT: [],
        }
        for insight_class in INSIGHTS:
            # TODO: Skip insight based on config

            run_insight, message = insight_class.has_all_required_data(
                has_manifest=self.manifest_present,
                has_catalog=self.catalog_present,
                has_run_results=self.run_results_present,
            )

            if run_insight:
                self.logger.info(f"Running insight {insight_class.NAME}")
                insight = insight_class(
                    manifest_wrapper=self.manifest_wrapper,
                    catalog_wrapper=self.catalog_wrapper,
                    nodes=self.nodes,
                    macros=self.macros,
                    sources=self.sources,
                    exposures=self.exposures,
                    children_map=self.children_map,
                    tests=self.tests,
                    project_name=self.project_name,
                    config=self.config,
                    selected_models=self.selected_models,
                    excluded_models=self.excluded_models,
                )

                if self._check_if_skipped(insight):
                    self.logger.info(
                        color_text(
                            f"Skipping insight {insight_class.NAME} as it is not enabled in config",
                            YELLOW,
                        )
                    )
                    continue
                try:
                    insights = insight.generate()
                    num_insights = len(insights)
                    text = f"Found {num_insights} insights for {insight_class.NAME}"
                    if num_insights > 0:
                        self.logger.info(color_text(text, RED))
                    else:
                        self.logger.info(f"No insights found for {insight_class.NAME}")

                    for insight in insights:
                        # Handle MODEL level insights
                        if insight.insight_level == MODEL:
                            # Add the insight if the model is selected or if all models are selected
                            # if self.selected_models_flag and insight.unique_id in self.selected_models or not self.selected_models_flag:
                            reports[MODEL].setdefault(insight.unique_id, []).append(insight)
                        # Handle PROJECT level insights, only if all models are selected
                        elif insight.insight_level == PROJECT:
                            reports[PROJECT].append(insight)

                except Exception as e:
                    self.logger.info(
                        color_text(
                            f"Error running insight {insight_class.NAME}: {e}. Skipping insight. {message}",
                            RED,
                        )
                    )
            else:
                self.logger.info(color_text(f"Skipping insight {insight_class.NAME} as {message}", YELLOW))

        return reports


def main(argv: Optional[Sequence[str]] = None):
    # start_time = time.time()
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--config-path",
        nargs="*",
        help="Path of the config file to be used for the insight generation",
    )

    args = parser.parse_known_args(argv)

    print(args)
    print(args[0].config_path)
    print(args[1])
    # config = None
    # if args.config_path:
    #     print(f"Using config file: {args.config_path}")
    #     config = load_config(args.config_path)

    # changed_files = get_changed_files()
    # tmp_folder = get_tmp_dir_path()
    # manifest_path = Path(tmp_folder / "manifest.json")
    # catalog_path = Path(tmp_folder / "catalog.json")
    # print(manifest_path, catalog_path)
    # generate_partial_manifest_catalog(
    #     changed_files,
    #     manifest_path=manifest_path,
    #     catalog_path=catalog_path,
    # )
    # config = {}
    # insight_generator = DBTInsightGenerator(manifest_path=manifest_path, catalog_path=catalog_path, config=config)
    # reports = insight_generator.run()
    # if reports:
    #     for model, insights in reports[MODEL].items():
    #         print(model)
    #         print(insights)
    #         print("\n\n\n")
    #     for insight in reports[PROJECT]:
    #         print(PROJECT)
    #         print(insight)
    #         print("\n\n\n")

    # end_time = time.time()
    # total_time = end_time - start_time
    # print(f"Total time taken: {round(total_time, 2)} seconds")


if __name__ == "__main__":
    exit(main())
