"""Tests for run_results v6 parser, specifically the resilient run `Status` enum.

Regression coverage for dbt 2.0 emitting ``status="reused"`` (and future unknown
statuses), which previously raised a ``ValidationError`` and caused the entire
run_results.json to be silently dropped during ingestion.
"""
from vendor.dbt_artifacts_parser.parser import parse_run_results
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v6 import Result
from vendor.dbt_artifacts_parser.parsers.run_results.run_results_v6 import Status

V6_SCHEMA = "https://schemas.getdbt.com/dbt/run-results/v6.json"


def _result(status: str, unique_id: str) -> dict:
    return {
        "status": status,
        "timing": [],
        "thread_id": "Thread-1",
        "execution_time": 0.1,
        "adapter_response": {},
        "unique_id": unique_id,
    }


def _run_results(*statuses: str) -> dict:
    return {
        "metadata": {
            "dbt_schema_version": V6_SCHEMA,
            "dbt_version": "2.0.0",
            "invocation_id": "test-invocation-123",
        },
        "elapsed_time": 1.5,
        "args": {},
        "results": [_result(s, f"model.proj.m{i}") for i, s in enumerate(statuses)],
    }


class TestRunResultStatus:
    """The run `Status` enum must accept new/unknown dbt statuses without failing."""

    def test_reused_status_parses(self):
        """dbt 2.0 emits `reused` for unchanged models; it must not fail validation."""
        result = Result(**_result("reused", "model.proj.a"))
        assert result.status.value == "reused"
        assert result.status is Status.reused

    def test_known_statuses_preserve_value(self):
        """Known run statuses still resolve with the correct `.value`."""
        for status in ("success", "error", "skipped", "partial success"):
            assert Result(**_result(status, "model.proj.a")).status.value == status

    def test_unknown_future_status_parses(self):
        """Forward-compat: a status dbt has not shipped yet still parses via `_missing_`."""
        result = Result(**_result("some_future_status", "model.proj.a"))
        assert result.status.value == "some_future_status"

    def test_test_and_freshness_statuses_keep_their_value(self):
        """Test/freshness statuses must keep their `.value` (resolved via Status1/Status2)."""
        for status in ("pass", "fail", "warn", "runtime error"):
            assert Result(**_result(status, "test.proj.t")).status.value == status


class TestParseRunResultsEntryPoint:
    """The public `parse_run_results` must parse a full file containing `reused`."""

    def test_file_with_reused_result_parses_fully(self):
        run_results = parse_run_results(_run_results("success", "reused", "skipped", "pass", "error", "no-op"))
        # Previously this whole file was dropped because of the single `reused` row.
        assert len(run_results.results) == 6
        assert {r.status.value for r in run_results.results} == {
            "success",
            "reused",
            "skipped",
            "pass",
            "error",
            "no-op",
        }
