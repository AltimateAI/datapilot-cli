"""Tests for run_results v1-v5 parsers, specifically the resilient run `Status` enum.

Mirrors ``test_run_results_v6.py``: the v6 `Status` shim (AI-7435) was never
applied to the pre-v6 schemas, so a `reused` (or any future unknown) run status
raised a ``ValidationError`` and the entire run_results.json was silently
dropped during ingestion (AI-7675 finding #2 residual).
"""
import importlib

import pytest

from vendor.dbt_artifacts_parser.parser import parse_run_results

VERSIONS = [1, 2, 3, 4, 5]


def _module(version: int):
    return importlib.import_module(f"vendor.dbt_artifacts_parser.parsers.run_results.run_results_v{version}")


def _result(status: str, unique_id: str) -> dict:
    return {
        "status": status,
        "timing": [],
        "thread_id": "Thread-1",
        "execution_time": 0.1,
        "adapter_response": {},
        "unique_id": unique_id,
    }


def _run_results(version: int, *statuses: str) -> dict:
    return {
        "metadata": {
            "dbt_schema_version": f"https://schemas.getdbt.com/dbt/run-results/v{version}.json",
            "dbt_version": "1.5.0",
            "invocation_id": "test-invocation-123",
        },
        "elapsed_time": 1.5,
        "args": {},
        "results": [_result(s, f"model.proj.m{i}") for i, s in enumerate(statuses)],
    }


@pytest.mark.parametrize("version", VERSIONS)
class TestRunResultStatusPreV6:
    """The run `Status` enum must accept new/unknown dbt statuses without failing."""

    def test_reused_status_parses(self, version):
        mod = _module(version)
        result = mod.RunResultOutput(**_result("reused", "model.proj.a"))
        assert result.status.value == "reused"
        assert result.status is mod.Status.reused

    def test_known_statuses_preserve_value(self, version):
        mod = _module(version)
        for status in ("success", "error", "skipped"):
            assert mod.RunResultOutput(**_result(status, "model.proj.a")).status.value == status

    def test_unknown_future_status_parses(self, version):
        mod = _module(version)
        result = mod.RunResultOutput(**_result("some_future_status", "model.proj.a"))
        assert result.status.value == "some_future_status"

    def test_test_and_freshness_statuses_keep_their_value(self, version):
        """Test/freshness statuses must keep their `.value` (resolved via Status1/Status2)."""
        mod = _module(version)
        for status in ("pass", "fail", "warn", "runtime error"):
            assert mod.RunResultOutput(**_result(status, "test.proj.t")).status.value == status


@pytest.mark.parametrize("version", VERSIONS)
class TestParseRunResultsEntryPointPreV6:
    """The public `parse_run_results` must parse a full file containing `reused`."""

    def test_file_with_reused_result_parses_fully(self, version):
        run_results = parse_run_results(_run_results(version, "success", "reused", "skipped", "pass", "error", "no-op"))
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
