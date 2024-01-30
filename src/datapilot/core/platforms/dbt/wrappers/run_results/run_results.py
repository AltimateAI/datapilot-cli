from abc import ABC
from abc import abstractmethod

from dbt_artifacts_parser.parsers.run_results.run_results_v1 import RunResultsV1
from dbt_artifacts_parser.parsers.run_results.run_results_v2 import RunResultsV2
from dbt_artifacts_parser.parsers.run_results.run_results_v3 import RunResultsV3
from dbt_artifacts_parser.parsers.run_results.run_results_v4 import RunResultsV4
from dbt_artifacts_parser.parsers.run_results.run_results_v5 import RunResultsV5


class BaseRunResultsWrapper(ABC):
    @abstractmethod
    def get_run_results(self):
        pass


class RunResultsV1Wrapper(BaseRunResultsWrapper):
    def __init__(self, run_results: RunResultsV1):
        self.run_results = run_results


class RunResultsV2Wrapper(BaseRunResultsWrapper):
    def __init__(self, run_results: RunResultsV2):
        self.run_results = run_results


class RunResultsV3Wrapper(BaseRunResultsWrapper):
    def __init__(self, run_results: RunResultsV3):
        self.run_results = run_results


class RunResultsV4Wrapper(BaseRunResultsWrapper):
    def __init__(self, run_results: RunResultsV4):
        self.run_results = run_results


class RunResultsV5Wrapper(BaseRunResultsWrapper):
    def __init__(self, run_results: RunResultsV5):
        self.run_results = run_results
