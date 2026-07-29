"""Tests for sources v3 parser: resilient freshness Status enums.

Regression coverage for dbt 2.0 (>= 2.0.0-preview.202) emitting CAPITALIZED
freshness statuses ("Pass", "Error") where earlier dbt emitted lowercase.
Previously every result failed validation and the entire sources.json was
silently dropped during ingestion (observed live: harvestgroup, 50 runs/day).
Same defect class as run_results_v6 ``reused`` (AI-7435 / PR #106).
"""
from vendor.dbt_artifacts_parser.parser import parse_sources
from vendor.dbt_artifacts_parser.parsers.sources.sources_v3 import Status
from vendor.dbt_artifacts_parser.parsers.sources.sources_v3 import Status1

V3_SCHEMA = "https://schemas.getdbt.com/dbt/sources/v3.json"


def _freshness_result(status: str, unique_id: str) -> dict:
    return {
        "unique_id": unique_id,
        "max_loaded_at": "2026-07-28T00:00:00Z",
        "snapshotted_at": "2026-07-28T01:00:00Z",
        "max_loaded_at_time_ago_in_s": 3600.0,
        "status": status,
        "criteria": {
            "warn_after": {"count": 12, "period": "hour"},
            "error_after": {"count": 24, "period": "hour"},
        },
        "adapter_response": {},
        "timing": [],
        "thread_id": "Thread-1",
        "execution_time": 1.0,
    }


def _sources(results: list) -> dict:
    return {
        "metadata": {
            "dbt_schema_version": V3_SCHEMA,
            "dbt_version": "2.0.0-preview.202",
            "generated_at": "2026-07-28T01:00:00Z",
            "invocation_id": "11111111-1111-1111-1111-111111111111",
            "env": {},
        },
        "results": results,
        "elapsed_time": 2.0,
    }


def test_capitalized_pass_and_error_parse_and_canonicalize():
    """The exact live failure: dbt 2.0 'Pass'/'Error' must parse, and known
    statuses canonicalize to their lowercase members."""
    parsed = parse_sources(
        _sources(
            [
                _freshness_result("Pass", "source.p.s.a"),
                _freshness_result("Error", "source.p.s.b"),
            ]
        )
    )
    statuses = [r.status for r in parsed.results]
    assert statuses[0] is Status1.pass_
    assert statuses[0].value == "pass"
    assert statuses[1] is Status1.error
    assert statuses[1].value == "error"


def test_lowercase_statuses_still_parse_unchanged():
    parsed = parse_sources(
        _sources(
            [
                _freshness_result("pass", "source.p.s.a"),
                _freshness_result("warn", "source.p.s.b"),
                _freshness_result("error", "source.p.s.c"),
            ]
        )
    )
    assert [r.status.value for r in parsed.results] == ["pass", "warn", "error"]


def test_unknown_future_status_surfaces_as_member():
    """Forward-compat: a status dbt invents later must not drop the file."""
    parsed = parse_sources(_sources([_freshness_result("Stale", "source.p.s.a")]))
    assert parsed.results[0].status.value == "Stale"


def test_runtime_error_arm_canonicalizes_case_but_stays_narrow():
    """The runtime-error union arm accepts case variants of its one value and
    nothing else — it must not swallow freshness rows."""
    assert Status("Runtime Error") is Status.runtime_error
    assert Status1("Runtime Error") is Status1.runtime_error
    runtime_row = {"unique_id": "source.p.s.x", "status": "runtime error", "error": "boom"}
    parsed = parse_sources(_sources([runtime_row]))
    assert parsed.results[0].status.value == "runtime error"
