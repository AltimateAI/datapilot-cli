from typing import Dict
from typing import Optional
from typing import Set

from dbt_artifacts_parser.parsers.manifest.manifest_v10 import GenericTestNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import ManifestV10
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import SingularTestNode

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
from datapilot.core.platforms.dbt.schemas.manifest import AltimateMacroArgument
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestColumnInfo
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestExposureNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestMacroNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestSourceNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestTestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateMaturityEnum
from datapilot.core.platforms.dbt.schemas.manifest import AltimateNodeConfig
from datapilot.core.platforms.dbt.schemas.manifest import AltimateOwner
from datapilot.core.platforms.dbt.schemas.manifest import AltimateQuoting
from datapilot.core.platforms.dbt.schemas.manifest import AltimateRefArgs
from datapilot.core.platforms.dbt.schemas.manifest import AltimateResourceType
from datapilot.core.platforms.dbt.schemas.manifest import AltimateSeedConfig
from datapilot.core.platforms.dbt.schemas.manifest import AltimateSeedNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateSourceConfig
from datapilot.core.platforms.dbt.schemas.manifest import AltimateTestConfig
from datapilot.core.platforms.dbt.schemas.manifest import AltimateTestMetadata
from datapilot.core.platforms.dbt.wrappers.manifest.v10.schemas import TEST_TYPE_TO_NODE_MAP
from datapilot.core.platforms.dbt.wrappers.manifest.v10.schemas import ExposureNode
from datapilot.core.platforms.dbt.wrappers.manifest.v10.schemas import MacroNode
from datapilot.core.platforms.dbt.wrappers.manifest.v10.schemas import ManifestNode
from datapilot.core.platforms.dbt.wrappers.manifest.v10.schemas import SeedNodeMap
from datapilot.core.platforms.dbt.wrappers.manifest.v10.schemas import SourceNode
from datapilot.core.platforms.dbt.wrappers.manifest.v10.schemas import TestNode
from datapilot.core.platforms.dbt.wrappers.manifest.wrapper import BaseManifestWrapper


class ManifestV10Wrapper(BaseManifestWrapper):
    def __init__(self, manifest: ManifestV10):
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
            compiled_code = node.compiled_code
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
            meta=node.meta,
            patch_path=node.patch_path,
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

    def _get_macro(self, macro: MacroNode) -> AltimateManifestMacroNode:
        return AltimateManifestMacroNode(
            name=macro.name,
            resource_type=AltimateResourceType(macro.resource_type.value),
            package_name=macro.package_name,
            path=macro.path,
            original_file_path=macro.original_file_path,
            unique_id=macro.unique_id,
            macro_sql=macro.macro_sql,
            depends_on=(
                AltimateDependsOn(
                    macros=macro.depends_on.macros,
                )
                if macro.depends_on
                else None
            ),
            description=macro.description,
            meta=macro.meta,
            docs=macro.docs,
            patch_path=macro.patch_path,
            arguments=[AltimateMacroArgument(**arg.dict()) for arg in macro.arguments] if macro.arguments else None,
            created_at=macro.created_at,
            supported_languages=macro.supported_languages,
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

    def _get_seed(self, seed: SeedNodeMap) -> AltimateSeedNode:
        return AltimateSeedNode(
            database=seed.database,
            schema_name=seed.schema_,
            name=seed.name,
            resource_type=AltimateResourceType(seed.resource_type.value),
            package_name=seed.package_name,
            path=seed.path,
            original_file_path=seed.original_file_path,
            unique_id=seed.unique_id,
            fqn=seed.fqn,
            alias=seed.alias,
            checksum=AltimateFileHash(
                name=seed.checksum.name,
                checksum=seed.checksum.checksum,
            )
            if seed.checksum
            else None,
            config=AltimateSeedConfig(**seed.config.dict()) if seed.config else None,
            description=seed.description,
            tags=seed.tags,
            columns={
                name: AltimateManifestColumnInfo(
                    name=column.name,
                    description=column.description,
                    meta=column.meta,
                    data_type=column.data_type,
                    quote=column.quote,
                    tags=column.tags,
                )
                for name, column in seed.columns.items()
            }
            if seed.columns
            else None,
            meta=seed.meta,
            group=seed.group,
            docs=seed.docs.dict() if seed.docs else None,
            patch_path=seed.patch_path,
            build_path=seed.build_path,
            deferred=seed.deferred,
            unrendered_config=seed.unrendered_config,
            created_at=seed.created_at,
            config_call_dict=seed.config_call_dict,
        )

    def get_nodes(
        self,
    ) -> Dict[str, AltimateManifestNode]:
        nodes = {}
        for node in self.manifest.nodes.values():
            if (
                node.resource_type.value
                in [
                    AltimateResourceType.seed.value,
                    AltimateResourceType.test.value,
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

    def get_macros(self) -> Dict[str, AltimateManifestMacroNode]:
        macros = {}
        for macro in self.manifest.macros.values():
            if macro.resource_type.value == AltimateResourceType.macro.value and macro.package_name == self.get_package():
                macros[macro.unique_id] = self._get_macro(macro)
        return macros

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

    def get_seeds(self) -> Dict[str, AltimateSeedNode]:
        seeds = {}
        for seed in self.manifest.nodes.values():
            if seed.resource_type.value == AltimateResourceType.seed.value:
                seeds[seed.unique_id] = self._get_seed(seed)
        return seeds

    def get_adapter_type(self) -> Optional[str]:
        return self.manifest.metadata.adapter_type

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
