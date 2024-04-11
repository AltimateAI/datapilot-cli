from typing import Dict
from typing import Type
from typing import Union

from dbt_artifacts_parser.parsers.manifest.manifest_v10 import AnalysisNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import Exposure
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import GenericTestNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import HookNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import Macro
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import ModelNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import RPCNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import SeedNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import SingularTestNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import SnapshotNode
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import SourceDefinition
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import SqlNode

from datapilot.core.platforms.dbt.constants import GENERIC
from datapilot.core.platforms.dbt.constants import SINGULAR

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
