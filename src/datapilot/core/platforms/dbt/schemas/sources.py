from typing import Union

from vendor.dbt_artifacts_parser.parsers.sources.sources_v1 import SourcesV1
from vendor.dbt_artifacts_parser.parsers.sources.sources_v2 import SourcesV2
from vendor.dbt_artifacts_parser.parsers.sources.sources_v3 import SourcesV3

Sources = Union[
    SourcesV3,
    SourcesV2,
    SourcesV1,
]
