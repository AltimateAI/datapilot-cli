from typing import Dict
from typing import Type
from typing import Union

from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Macros
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes1
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes2
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes3
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes5
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes6
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes7
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Sources

from datapilot.core.platforms.dbt.constants import GENERIC
from datapilot.core.platforms.dbt.constants import SINGULAR

ManifestNode = Union[Nodes, Nodes1, Nodes2, Nodes3, Nodes4, Nodes5, Nodes6, Nodes7]

SourceNode = Sources

ExposureNode = Exposures

TestNode = Union[Nodes6, Nodes2]

MacroNode = Macros

TEST_TYPE_TO_NODE_MAP: Dict[str, Type] = {
    GENERIC: [Nodes6],
    SINGULAR: [Nodes2],
}


SeedNodeMap = Nodes
