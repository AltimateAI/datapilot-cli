from typing import Union

from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v1 import RunResultsV1
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v2 import RunResultsV2
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v3 import RunResultsV3
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v4 import RunResultsV4
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v5 import RunResultsV5
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v6 import RunResultsV6

RunResults = Union[
    RunResultsV6,
    RunResultsV5,
    RunResultsV4,
    RunResultsV3,
    RunResultsV2,
    RunResultsV1,
]
