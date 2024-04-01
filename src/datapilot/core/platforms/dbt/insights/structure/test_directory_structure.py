from typing import List
from typing import Optional

from datapilot.core.insights.utils import get_severity
from datapilot.core.platforms.dbt.insights.schema import DBTInsightResult
from datapilot.core.platforms.dbt.insights.schema import DBTModelInsightResponse
from datapilot.core.platforms.dbt.insights.structure.base import DBTStructureInsight
from datapilot.utils.utils import get_dir_path


class DBTTestDirectoryStructure(DBTStructureInsight):
    """
    DBTTestDirectoryStructure checks if tests are placed in the correct directories.
    """

    NAME = "Bad test directory structure"
    ALIAS = "test_directory_structure"
    DESCRIPTION = "This rule checks if tests are correctly placed in the same directories as their corresponding models."
    REASON_TO_FLAG = (
        "It is important for tests to be placed in the same directory as their corresponding models to maintain "
        "a coherent and easy-to-navigate project structure. This practice enhances the ease of understanding "
        "and updating tests in parallel with model changes."
    )
    FAILURE_MESSAGE = (
        "Incorrect Test Placement Detected: The test `{model_unique_id}` is not in the correct directory. "
        "For consistent project structure and easy maintenance, it should be placed in the same directory as "
        "its corresponding model."
    )
    RECOMMENDATION = (
        "To rectify this, move the test `{model_unique_id}` to the directory `{convention}`, where its corresponding "
        "model is located. This adjustment will align your test's location with best practices for"
        " project organization."
    )

    def _build_failure_result(self, model_unique_id: str, convention: Optional[str]) -> DBTInsightResult:
        failure_message = self.FAILURE_MESSAGE.format(
            model_unique_id=model_unique_id,
        )
        return DBTInsightResult(
            name=self.NAME,
            type=self.TYPE,
            message=failure_message,
            recommendation=self.RECOMMENDATION.format(model_unique_id=model_unique_id, convention=convention),
            reason_to_flag=self.REASON_TO_FLAG,
            metadata={
                "model": model_unique_id,
                "convention": convention,
            },
        )

    def generate(self, *args, **kwargs) -> List[DBTModelInsightResponse]:
        insights = []
        for test_id, test in self.tests.items():
            if self.should_skip_model(test_id):
                self.logger.debug(f"Skipping model {test_id} as it is not enabled for selected models")
                continue
            test_file_path = get_dir_path(test_id)
            for node_id in test.depends_on.nodes:
                node = self.get_node(node_id)
                if not node:
                    continue
                expected_dir_path = get_dir_path(node_id)
                if expected_dir_path != test_file_path:
                    insights.append(
                        DBTModelInsightResponse(
                            unique_id=test_id,
                            package_name=test.package_name,
                            path=test.path,
                            original_file_path=test.original_file_path,
                            insight=self._build_failure_result(test_id, expected_dir_path),
                            severity=get_severity(self.config, self.ALIAS, self.DEFAULT_SEVERITY),
                        )
                    )
        return insights
