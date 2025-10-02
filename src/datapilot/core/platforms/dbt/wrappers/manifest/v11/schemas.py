from typing import Dict
from typing import Type
from typing import Union

from datapilot.core.platforms.dbt.constants import GENERIC
from datapilot.core.platforms.dbt.constants import SINGULAR
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import AnalysisNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import Exposure
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import GenericTestNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import HookNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import Macro
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import ModelNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import RPCNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import SeedNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import SingularTestNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import SnapshotNode
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import SourceDefinition
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v11 import SqlNode

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

SourceNode = SourceDefinition

ExposureNode = Exposure

TestNode = Union[GenericTestNode, SingularTestNode]

MacroNode = Macro

TEST_TYPE_TO_NODE_MAP: Dict[str, Type] = {
    GENERIC: [GenericTestNode],
    SINGULAR: [SingularTestNode],
}


SeedNodeMap = SeedNode
