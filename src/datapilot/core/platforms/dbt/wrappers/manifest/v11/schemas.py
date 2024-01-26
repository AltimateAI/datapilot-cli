from typing import Dict, Type, Union

from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
    AnalysisNode,
    Exposure,
    GenericTestNode,
    HookNode,
    ModelNode,
    RPCNode,
    SeedNode,
    SingularTestNode,
    SnapshotNode,
    SourceDefinition,
    SqlNode,
)

from datapilot.core.platforms.dbt.constants import GENERIC, SINGULAR

ManifestNode = Union[
    AnalysisNode,
    SingularTestNode,
    HookNode,
    ModelNode,
    RPCNode,
    SqlNode,
    GenericTestNode,
    SnapshotNode,
    SeedNode,
]

SourceNode = Union[SourceDefinition]

ExposureNode = Union[Exposure]

TestNode = Union[GenericTestNode, SingularTestNode]


TEST_TYPE_TO_NODE_MAP: dict[str, Type[GenericTestNode | SingularTestNode]] = {
    GENERIC: GenericTestNode,
    SINGULAR: SingularTestNode,
}
