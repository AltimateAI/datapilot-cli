import logging
from typing import Dict

# from src.utils.formatting.utils import generate_model_insights_table
from typing import Optional

from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import PROJECT
from datapilot.core.platforms.dbt.factory import DBTFactory
from datapilot.core.platforms.dbt.insights import INSIGHTS
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
        self.sources = self.manifest_wrapper.get_sources()
        self.exposures = self.manifest_wrapper.get_exposures()
        self.children_map = self.manifest_wrapper.parent_to_child_map(self.nodes)
        self.tests = self.manifest_wrapper.get_tests()
        self.project_name = self.manifest_wrapper.get_package()
        # TODO - add catalog and run results wrappers

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
                    sources=self.sources,
                    exposures=self.exposures,
                    children_map=self.children_map,
                    tests=self.tests,
                    project_name=self.project_name,
                    config=self.config,
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
                        if insight.insight_level == MODEL:
                            reports[MODEL].setdefault(insight.unique_id, []).append(insight)
                        else:
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
