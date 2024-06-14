from typing import Dict
from typing import Type
from typing import Union

from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Macros
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node1
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node2
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node3
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node4
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node5
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node6
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Node7
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Sources

from datapilot.core.platforms.dbt.constants import GENERIC
from datapilot.core.platforms.dbt.constants import SINGULAR

ManifestNode = Union[Node, Node1, Node2, Node3, Node4, Node5, Node6, Node7]

SourceNode = Sources

ExposureNode = Exposures

TestNode = Union[Node6, Node2]

MacroNode = Macros

TEST_TYPE_TO_NODE_MAP: Dict[str, Type] = {
    GENERIC: [Node6],
    SINGULAR: [Node2],
}


SeedNodeMap = Node
