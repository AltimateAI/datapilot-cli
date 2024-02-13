import logging

# from src.utils.formatting.utils import generate_model_insights_table
from typing import Dict
from typing import Optional

from configtree.tree import Tree
from requests import Response

from datapilot.clients.altimate.client import APIClient
from datapilot.core.platforms.dbt.factory import DBTFactory
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.utils import load_catalog
from datapilot.core.platforms.dbt.utils import load_manifest


class DBTSqlInsightGenerator:
    def __init__(
        self,
        manifest_path: str,
        adapter: str,
        catalog_path: Optional[str] = None,
        run_results_path: Optional[str] = None,
        env: Optional[str] = None,
        config: Optional[Tree] = None,
        target: str = "dev",
        api_client: APIClient = None,
    ):
        self.manifest_path = manifest_path
        self.catalog_path = catalog_path
        self.run_results_path = run_results_path
        self.target = target
        self.env = env
        self.adapter = adapter
        self.config = config or Tree()
        manifest = load_manifest(self.manifest_path)

        self.manifest_wrapper = DBTFactory.get_manifest_wrapper(manifest)
        self.manifest_present = True
        self.catalog_present = False

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
        self.api_client = api_client

    def _build_request(self):
        request = {
            "models": [],
            "config": dict(self.config) if self.config else {},
            "adapter": self.adapter,
        }
        request["config"] = dict(self.config)
        request["adapter"] = self.adapter
        for node_id, node in self.nodes.items():
            model = {}
            model["uniqueId"] = node_id
            model["name"] = node.name
            model["alias"] = node.alias
            model["database"] = node.database
            model["schema_name"] = node.schema_name
            model["package_name"] = node.package_name
            model["path"] = node.path
            model["original_file_path"] = node.original_file_path
            model["columns"] = []
            if self.catalog_present:
                model["columns"] = self.catalog_wrapper.get_columns(node_id)
            request["models"].append(
                {
                    "compiled_sql": node.compiled_code,
                    "model_node": model,
                }
            )
        return request

    def _parse_response(self, response: Dict[str, Dict]) -> Dict[str, DBTModelInsightResponse]:
        model_insights = {}
        for model_id, insights in response.items():
            model_insights[model_id] = [DBTModelInsightResponse(**insight) for insight in insights]
        return model_insights

    def run(self):
        reports = {}

        request = self._build_request()

        response: Response = self.api_client.sql_insights(data=request)

        if response.status_code == 200:
            return self._parse_response(response.json())
        else:
            self.logger.error(f"Error running SQL insights: {response.status_code}, {response.text}. Skipping SQL insights.")
            return reports
