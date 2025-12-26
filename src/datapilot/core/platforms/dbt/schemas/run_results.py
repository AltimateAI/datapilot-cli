from typing import Union

from pydantic import ConfigDict

from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v1 import RunResultsV1 as BaseRunResultsV1
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v2 import RunResultsV2 as BaseRunResultsV2
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v3 import RunResultsV3 as BaseRunResultsV3
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v4 import RunResultsV4 as BaseRunResultsV4
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v5 import RunResultsV5 as BaseRunResultsV5
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v6 import RunResultsV6 as BaseRunResultsV6


class RunResultsV1(BaseRunResultsV1):
    model_config = ConfigDict(extra="allow")


class RunResultsV2(BaseRunResultsV2):
    model_config = ConfigDict(extra="allow")


class RunResultsV3(BaseRunResultsV3):
    model_config = ConfigDict(extra="allow")


class RunResultsV4(BaseRunResultsV4):
    model_config = ConfigDict(extra="allow")


class RunResultsV5(BaseRunResultsV5):
    model_config = ConfigDict(extra="allow")


class RunResultsV6(BaseRunResultsV6):
    model_config = ConfigDict(extra="allow")


RunResults = Union[
    RunResultsV6,
    RunResultsV5,
    RunResultsV4,
    RunResultsV3,
    RunResultsV2,
    RunResultsV1,
]
