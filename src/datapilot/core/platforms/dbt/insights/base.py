from abc import abstractmethod
from typing import ClassVar
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from datapilot.config.utils import get_insight_config
from datapilot.core.insights.base.insight import Insight
from datapilot.core.insights.schema import Severity
from datapilot.core.platforms.dbt.constants import NON_MATERIALIZED
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestExposureNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestMacroNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestSourceNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestTestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.schemas.manifest import AltimateSeedNode
from datapilot.core.platforms.dbt.wrappers.manifest.wrapper import BaseManifestWrapper


class DBTInsight(Insight):
    DEFAULT_SEVERITY = Severity.ERROR
    FILES_REQUIRED: ClassVar = ["Manifest"]

    def __init__(
        self,
        manifest_wrapper: BaseManifestWrapper,
        nodes: Dict[str, AltimateManifestNode],
        sources: Dict[str, AltimateManifestSourceNode],
        exposures: Dict[str, AltimateManifestExposureNode],
        tests: Dict[str, AltimateManifestTestNode],
        seeds: Dict[str, AltimateSeedNode],
        macros: Dict[str, AltimateManifestMacroNode],
        children_map: Dict[str, List[str]],
        project_name: str,
        adapter_type: Optional[str],
        selected_models: Union[List[str], None] = None,
        excluded_models: Union[List[str], None] = None,
        *args,
        **kwargs,
    ):
        self.manifest = manifest_wrapper
        self.nodes = nodes
        self.macros = macros or {}
        self.sources = sources
        self.exposures = exposures
        self.tests = tests
        self.seeds = seeds
        self.children_map = children_map
        self.project_name = project_name
        self.adapter_type = adapter_type
        self.selected_models = selected_models
        self.excluded_models = excluded_models
        super().__init__(*args, **kwargs)

    @abstractmethod
    def generate(self, *args, **kwargs) -> Dict:
        pass

    def check_part_of_project(self, node_project_name: str) -> bool:
        return node_project_name == self.project_name

    def get_node(
        self, node_id: str
    ) -> Union[
        AltimateManifestNode, AltimateManifestSourceNode, AltimateManifestExposureNode, AltimateManifestTestNode, AltimateManifestMacroNode
    ]:
        if node_id in self.nodes:
            return self.nodes[node_id]
        elif node_id in self.sources:
            return self.sources[node_id]
        elif node_id in self.exposures:
            return self.exposures[node_id]
        elif node_id in self.tests:
            return self.tests[node_id]
        elif node_id in self.macros:
            return self.macros[node_id]
        elif node_id in self.seeds:
            return self.seeds[node_id]
        else:
            self.logger.debug(f"Model {node_id} not found in manifest")
            return None

    def find_long_chains(self, min_chain_length=4):
        """
        Find chains of nodes with 'materialized' set to 'view' or 'ephemeral' of a given minimum length.

        :param nodes: Dictionary of nodes where key is node_id and value is a node with 'depends_on' and 'materialized'.
        :param min_chain_length: Minimum length of the chain to be found.
        :return: A list of chains, where each chain is a list of node IDs.
        """

        def is_not_materialized(node: Union[AltimateManifestNode, AltimateManifestSourceNode]) -> bool:
            if node.resource_type == AltimateResourceType.source:
                return False
            return node.config.materialized in NON_MATERIALIZED

        def build_chain(node_id, current_chain):
            if len(current_chain) >= min_chain_length:
                long_chains.append(current_chain)
                return
            for parent_id in self.get_node(node_id).depends_on.nodes:
                if is_not_materialized(self.get_node(parent_id)):
                    build_chain(parent_id, [*current_chain, parent_id])

        long_chains = []
        for node_id, node in self.nodes.items():
            if is_not_materialized(node):
                build_chain(node_id, [node_id])

        return long_chains

    def should_skip_model(self, model_unique_id):
        """Check if a model is in the excluded models list."""
        if self.selected_models:
            return model_unique_id not in self.selected_models

        return False

    @classmethod
    def get_config_schema(cls):
        return {
            "name": cls.NAME,
            "alias": cls.ALIAS,
            "type": cls.TYPE,
            "files_required": cls.FILES_REQUIRED,
            "description": cls.DESCRIPTION,
            "config": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": {}},
        }

    def requires_catalog(cls) -> bool:
        return False

    def get_check_config(self, key: str) -> any:
        return get_insight_config(self.config, self.ALIAS, key)
