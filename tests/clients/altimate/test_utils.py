from datapilot.clients.altimate.constants import SUPPORTED_ARTIFACT_TYPES
from datapilot.clients.altimate.utils import onboard_file


class TestOnboardFile:
    def test_supported_artifact_types(self):
        """Test that all expected artifact types are supported."""
        expected_types = {"manifest", "catalog", "run_results", "sources", "semantic_manifest"}
        assert SUPPORTED_ARTIFACT_TYPES == expected_types

    def test_unsupported_file_type_returns_error(self):
        """Test that unsupported file types return an error without making API calls."""
        test_token = "test_token"  # noqa: S105
        result = onboard_file(
            api_token=test_token,
            tenant="test_tenant",
            dbt_core_integration_id="test_id",
            dbt_core_integration_environment="PROD",
            file_type="unsupported_type",
            file_path="test_path.json",
            backend_url="http://localhost",
        )

        assert result["ok"] is False
        assert "Unsupported file type" in result["message"]
        assert "unsupported_type" in result["message"]
