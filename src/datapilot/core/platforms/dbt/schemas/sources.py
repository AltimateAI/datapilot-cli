from typing import Union

from pydantic import ConfigDict

from vendor.dbt_artifacts_parser.parsers.sources.sources_v1 import SourcesV1 as BaseSourcesV1
from vendor.dbt_artifacts_parser.parsers.sources.sources_v2 import SourcesV2 as BaseSourcesV2
from vendor.dbt_artifacts_parser.parsers.sources.sources_v3 import SourcesV3 as BaseSourcesV3


class SourcesV1(BaseSourcesV1):
    model_config = ConfigDict(extra="allow")


class SourcesV2(BaseSourcesV2):
    model_config = ConfigDict(extra="allow")


class SourcesV3(BaseSourcesV3):
    model_config = ConfigDict(extra="allow")


Sources = Union[
    SourcesV3,
    SourcesV2,
    SourcesV1,
]
