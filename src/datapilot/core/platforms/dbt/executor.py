import logging

# from src.utils.formatting.utils import generate_model_insights_table
from typing import Dict
from typing import List
from typing import Optional

from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.exceptions import AltimateCLIArgumentError
from datapilot.core.platforms.dbt.factory import DBTFactory
from datapilot.core.platforms.dbt.insights import INSIGHTS
from datapilot.core.platforms.dbt.schemas.manifest import Catalog
from datapilot.core.platforms.dbt.schemas.manifest import Manifest
from datapilot.core.platforms.dbt.utils import get_models
from datapilot.utils.formatting.utils import RED
from datapilot.utils.formatting.utils import YELLOW
from datapilot.utils.formatting.utils import color_text


class DBTInsightGenerator:
    def __init__(
        self,
        manifest: Manifest,
        catalog: Optional[Catalog] = None,
        run_results_path: Optional[str] = None,
        env: Optional[str] = None,
        config: Optional[Dict] = None,
        target: str = "dev",
        selected_models: Optional[str] = None,
        selected_model_ids: Optional[List[str]] = None,
    ):
        self.run_results_path = run_results_path
        self.target = target
        self.env = env
        self.config = config or {}

        self.manifest_wrapper = DBTFactory.get_manifest_wrapper(manifest)
        self.manifest_present = True
        self.catalog_present = False
        self.catalog_wrapper = None

        if catalog:
            self.catalog_wrapper = DBTFactory.get_catalog_wrapper(catalog)
            self.catalog_present = True

        self.run_results_present = False
        self.logger = logging.getLogger("dbt-insight-generator")

        self.nodes = self.manifest_wrapper.get_nodes()
        self.macros = self.manifest_wrapper.get_macros()
        self.sources = self.manifest_wrapper.get_sources()
        self.exposures = self.manifest_wrapper.get_exposures()
        self.seeds = self.manifest_wrapper.get_seeds()
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
        if selected_model_ids:
            self.selected_models_flag = True
            self.selected_models = selected_model_ids
        elif selected_models:
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
                    seeds=self.seeds,
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
