from typing import Dict
from typing import Set

from dbt_artifacts_parser.parsers.manifest.manifest_v11 import GenericTestNode
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import ManifestV11
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import SingularTestNode

from datapilot.core.platforms.dbt.constants import GENERIC
from datapilot.core.platforms.dbt.constants import OTHER_TEST_NODE
from datapilot.core.platforms.dbt.constants import SEED
from datapilot.core.platforms.dbt.constants import SINGULAR
from datapilot.core.platforms.dbt.schemas.manifest import AltimateDBTContract
from datapilot.core.platforms.dbt.schemas.manifest import AltimateDependsOn
from datapilot.core.platforms.dbt.schemas.manifest import AltimateExposureType
from datapilot.core.platforms.dbt.schemas.manifest import AltimateExternalTable
from datapilot.core.platforms.dbt.schemas.manifest import AltimateFileHash
from datapilot.core.platforms.dbt.schemas.manifest import AltimateFreshnessThreshold
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestColumnInfo
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestExposureNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestSourceNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestTestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateMaturityEnum
from datapilot.core.platforms.dbt.schemas.manifest import AltimateNodeConfig
from datapilot.core.platforms.dbt.schemas.manifest import AltimateOwner
from datapilot.core.platforms.dbt.schemas.manifest import AltimateQuoting
from datapilot.core.platforms.dbt.schemas.manifest import AltimateRefArgs
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.schemas.manifest import AltimateSourceConfig
from datapilot.core.platforms.dbt.schemas.manifest import AltimateTestConfig
from datapilot.core.platforms.dbt.schemas.manifest import AltimateTestMetadata
from datapilot.core.platforms.dbt.wrappers.manifest.v11.schemas import TEST_TYPE_TO_NODE_MAP
from datapilot.core.platforms.dbt.wrappers.manifest.v11.schemas import ExposureNode
from datapilot.core.platforms.dbt.wrappers.manifest.v11.schemas import ManifestNode
from datapilot.core.platforms.dbt.wrappers.manifest.v11.schemas import SourceNode
from datapilot.core.platforms.dbt.wrappers.manifest.v11.schemas import TestNode
from datapilot.core.platforms.dbt.wrappers.manifest.wrapper import BaseManifestWrapper


class ManifestV11Wrapper(BaseManifestWrapper):
    def __init__(self, manifest: ManifestV11):
        self.manifest = manifest

    def _get_node(self, node: ManifestNode) -> AltimateManifestNode:
        (
            sources,
            metrics,
            compiled_path,
            compiled,
            compiled_code,
            depends_on_nodes,
            depends_on_macros,
            raw_code,
            language,
            contract,
        ) = ([], [], None, None, None, None, None, "", "", None)
        if node.resource_type.value != SEED:
            sources = node.sources
            metrics = node.metrics
            depends_on_nodes = node.depends_on.nodes if node.depends_on else None
            depends_on_macros = node.depends_on.macros if node.depends_on else None
            compiled_path = node.compiled_path
            compiled = node.compiled
            raw_code = node.raw_code
            language = node.language
            contract = AltimateDBTContract(**node.contract.__dict__) if node.contract else None

        return AltimateManifestNode(
            database=node.database,
            schema_name=node.schema_,
            name=node.name,
            resource_type=AltimateResourceType(node.resource_type.value),
            package_name=node.package_name,
            path=node.path,
            description=node.description,
            original_file_path=node.original_file_path,
            unique_id=node.unique_id,
            fqn=node.fqn,
            alias=node.alias,
            raw_code=raw_code,
            language=language,
            config=AltimateNodeConfig(**node.config.__dict__) if node.config else None,
            checksum=AltimateFileHash(
                name=node.checksum.name if node.checksum else None,
                checksum=node.checksum.checksum if node.checksum else None,
            ),
            columns={
                name: AltimateManifestColumnInfo(
                    name=column.name,
                    description=column.description,
                    meta=column.meta,
                    data_type=column.data_type,
                    quote=column.quote,
                    tags=column.tags,
                )
                for name, column in node.columns.items()
            },
            relation_name=node.relation_name,
            sources=sources,
            metrics=metrics,
            depends_on=AltimateDependsOn(
                nodes=depends_on_nodes,
                macros=depends_on_macros,
            ),
            compiled_path=compiled_path,
            compiled=compiled,
            compiled_code=compiled_code,
            contract=contract,
        )

    def _get_source(self, source: SourceNode) -> AltimateManifestSourceNode:
        return AltimateManifestSourceNode(
            database=source.database,
            resource_type=AltimateResourceType(source.resource_type.value),
            schema_name=source.schema_,
            name=source.name,
            package_name=source.package_name,
            path=source.path,
            original_file_path=source.original_file_path,
            unique_id=source.unique_id,
            fqn=source.fqn,
            source_name=source.source_name,
            source_description=source.source_description,
            loader=source.loader,
            identifier=source.identifier,
            quoting=AltimateQuoting(**source.quoting.dict()) if source.quoting else None,
            loaded_at_field=source.loaded_at_field,
            freshness=AltimateFreshnessThreshold(**source.freshness.dict()) if source.freshness else None,
            external=AltimateExternalTable(**source.external.dict()) if source.external else None,
            description=source.description,
            columns={
                name: AltimateManifestColumnInfo(
                    name=column.name,
                    description=column.description,
                    meta=column.meta,
                    data_type=column.data_type,
                    quote=column.quote,
                    tags=column.tags,
                )
                for name, column in source.columns.items()
            },
            meta=source.meta,
            relation_name=source.relation_name,
            source_meta=source.source_meta,
            tags=source.tags,
            config=AltimateSourceConfig(**source.config.dict()) if source.config else None,
            patch_path=source.patch_path,
            unrendered_config=source.unrendered_config,
            created_at=source.created_at,
        )

    def _get_exposure(self, exposure: ExposureNode) -> AltimateManifestExposureNode:
        return AltimateManifestExposureNode(
            name=exposure.name,
            resource_type=AltimateResourceType(exposure.resource_type.value),
            package_name=exposure.package_name,
            path=exposure.path,
            original_file_path=exposure.original_file_path,
            unique_id=exposure.unique_id,
            fqn=exposure.fqn,
            type=AltimateExposureType(exposure.type.value) if exposure.type else None,
            owner=AltimateOwner(**exposure.owner.dict()) if exposure.owner else None,
            description=exposure.description,
            label=exposure.label,
            maturity=AltimateMaturityEnum(exposure.maturity.value) if exposure.maturity else None,
            meta=exposure.meta,
            tags=exposure.tags,
            config=AltimateSourceConfig(**exposure.config.dict()) if exposure.config else None,
            unrendered_config=exposure.unrendered_config,
            url=exposure.url,
            depends_on=AltimateDependsOn(
                nodes=exposure.depends_on.nodes,
                macros=exposure.depends_on.macros,
            )
            if exposure.depends_on
            else None,
            refs=[AltimateRefArgs(**ref.dict()) for ref in exposure.refs] if exposure.refs else None,
            sources=exposure.sources,
            metrics=exposure.metrics,
            created_at=exposure.created_at,
        )

    def _get_tests(self, test: TestNode) -> AltimateManifestTestNode:
        test_metadata = None
        if isinstance(test, GenericTestNode):
            test_type = GENERIC
            test_metadata = AltimateTestMetadata(**test.test_metadata.dict()) if test.test_metadata else None
        elif isinstance(test, SingularTestNode):
            test_type = SINGULAR
        else:
            test_type = OTHER_TEST_NODE
        return AltimateManifestTestNode(
            test_metadata=test_metadata,
            test_type=test_type,
            name=test.name,
            resource_type=AltimateResourceType(test.resource_type.value),
            package_name=test.package_name,
            path=test.path,
            original_file_path=test.original_file_path,
            unique_id=test.unique_id,
            fqn=test.fqn,
            alias=test.alias,
            checksum=AltimateFileHash(
                name=test.checksum.name,
                checksum=test.checksum.checksum,
            )
            if test.checksum
            else None,
            config=AltimateTestConfig(**test.config.dict()) if test.config else None,
            description=test.description,
            tags=test.tags,
            columns={
                name: AltimateManifestColumnInfo(
                    name=column.name,
                    description=column.description,
                    meta=column.meta,
                    data_type=column.data_type,
                    quote=column.quote,
                    tags=column.tags,
                )
                for name, column in test.columns.items()
            }
            if test.columns
            else None,
            meta=test.meta,
            relation_name=test.relation_name,
            group=test.group,
            raw_code=test.raw_code,
            language=test.language,
            refs=[AltimateRefArgs(**ref.dict()) for ref in test.refs] if test.refs else None,
            sources=test.sources,
            metrics=test.metrics,
            depends_on=AltimateDependsOn(
                nodes=test.depends_on.nodes,
                macros=test.depends_on.macros,
            )
            if test.depends_on
            else None,
            compiled_path=test.compiled_path,
            compiled=test.compiled,
            compiled_code=test.compiled_code,
        )

    def get_nodes(
        self,
    ) -> Dict[str, AltimateManifestNode]:
        nodes = {}
        for node in self.manifest.nodes.values():
            if (
                node.resource_type
                in [
                    AltimateResourceType.seed,
                    AltimateResourceType.test,
                ]
                or node.package_name != self.get_package()
            ):
                continue
            nodes[node.unique_id] = self._get_node(node)
        return nodes

    def get_package(self) -> str:
        return self.manifest.metadata.project_name

    def get_sources(self) -> Dict[str, AltimateManifestSourceNode]:
        sources = {}
        for source in self.manifest.sources.values():
            sources[source.unique_id] = self._get_source(source)
        return sources

    def get_exposures(self) -> Dict[str, AltimateManifestExposureNode]:
        exposures = {}
        for exposure in self.manifest.exposures.values():
            exposures[exposure.unique_id] = self._get_exposure(exposure)
        return exposures

    def get_tests(self, type=None) -> Dict[str, AltimateManifestTestNode]:
        tests = {}
        # Initialize types_union with TestNode
        types = [GenericTestNode, SingularTestNode]

        # Add other types to the union if provided
        if type:
            types = TEST_TYPE_TO_NODE_MAP.get(type)

        for node in self.manifest.nodes.values():
            # Check if the node is a test and of the correct type
            if node.resource_type.value == AltimateResourceType.test.value:
                if any(isinstance(node, t) for t in types):
                    tests[node.unique_id] = self._get_tests(node)
        return tests

    def parent_to_child_map(self, nodes: Dict[str, AltimateManifestNode]) -> Dict[str, Set[str]]:
        """
        Current manifest contains information about parents
        THis gives an information of node to childre
        :param nodes: A dictionary of nodes in a manifest.
        :return: A dictionary of all the children of a node.
        """
        children_map = {}
        for node_id, node in nodes.items():
            if node_id not in children_map:
                children_map[node_id] = set()
            for parent in node.depends_on.nodes or []:
                children_map.setdefault(parent, set()).add(node_id)
        return children_map
