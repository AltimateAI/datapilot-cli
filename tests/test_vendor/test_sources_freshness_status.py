"""Tests for the sources v1-v3 parsers, specifically the resilient freshness `Status1` enum.

Regression coverage for the dbt Fusion engine emitting capitalized freshness
statuses (``"Pass"`` / ``"Warn"`` / ``"Error"``) in sources.json. Every result row
failed both members of the ``results`` union, so the ENTIRE sources.json raised a
``ValidationError`` and was silently dropped during ingestion -- no source freshness
ever reached Postgres for a Fusion-backed environment.

The statuses must fold to their canonical lowercase members, because the extractor
persists ``result.status.value`` and the rest of the platform (including every
dbt-core-backed tenant already in the same table) uses the lowercase vocabulary.
"""
import pytest

from vendor.dbt_artifacts_parser.parser import parse_sources
from vendor.dbt_artifacts_parser.parsers.sources.sources_v1 import SourceFreshnessOutput as OutputV1
from vendor.dbt_artifacts_parser.parsers.sources.sources_v1 import Status1 as StatusV1
from vendor.dbt_artifacts_parser.parsers.sources.sources_v2 import SourceFreshnessOutput as OutputV2
from vendor.dbt_artifacts_parser.parsers.sources.sources_v2 import Status1 as StatusV2
from vendor.dbt_artifacts_parser.parsers.sources.sources_v3 import Results as RuntimeErrorV3
from vendor.dbt_artifacts_parser.parsers.sources.sources_v3 import Results1 as OutputV3
from vendor.dbt_artifacts_parser.parsers.sources.sources_v3 import Status1 as StatusV3

V3_SCHEMA = "https://schemas.getdbt.com/dbt/sources/v3.json"

# (output model, status enum) per schema version -- the enum is identical in all three.
VERSIONS = [
    pytest.param(OutputV1, StatusV1, id="v1"),
    pytest.param(OutputV2, StatusV2, id="v2"),
    pytest.param(OutputV3, StatusV3, id="v3"),
]

# Fusion's Rust variant name -> the canonical lowercase status dbt-core emits.
FUSION_CASINGS = [("Pass", "pass"), ("Warn", "warn"), ("Error", "error")]


def _output(status: str, unique_id: str = "source.proj.schema.tbl") -> dict:
    """A complete freshness result -- the shape Fusion always emits."""
    return {
        "unique_id": unique_id,
        "max_loaded_at": "2026-08-19T08:33:50.855920Z",
        "snapshotted_at": "2026-08-19T17:00:33.833000Z",
        "max_loaded_at_time_ago_in_s": 30402.0,
        "status": status,
        "criteria": {
            "warn_after": {"count": 24, "period": "hour"},
            "error_after": {"count": 48, "period": "hour"},
        },
        "adapter_response": {},
        "timing": [],
        "thread_id": "Thread-20",
        "execution_time": 0.0,
    }


def _sources_v3(*statuses: str) -> dict:
    return {
        "metadata": {
            "dbt_schema_version": V3_SCHEMA,
            "dbt_version": "2.0.0-preview.210",
            "invocation_id": "test-invocation-123",
        },
        "elapsed_time": 1.5,
        "results": [_output(s, f"source.proj.sch.t{i}") for i, s in enumerate(statuses)],
    }


class TestFusionStatusCasing:
    """Fusion's capitalized statuses must parse AND normalize to lowercase."""

    @pytest.mark.parametrize(("model", "status_enum"), VERSIONS)
    @pytest.mark.parametrize(("fusion", "canonical"), FUSION_CASINGS)
    def test_capitalized_status_folds_to_canonical_member(self, model, status_enum, fusion, canonical):
        result = model(**_output(fusion))
        assert result.status is status_enum(canonical)
        assert result.status.value == canonical

    @pytest.mark.parametrize(("model", "status_enum"), VERSIONS)
    def test_lowercase_statuses_unchanged(self, model, status_enum):
        """dbt-core's existing lowercase vocabulary must keep resolving as before."""
        for status in ("pass", "warn", "error", "runtime error"):
            assert model(**_output(status)).status.value == status

    @pytest.mark.parametrize(("model", "status_enum"), VERSIONS)
    def test_unknown_future_status_parses(self, model, status_enum):
        """Forward-compat: a status dbt has not shipped yet must not drop the file."""
        assert model(**_output("some_future_status")).status.value == "some_future_status"


class TestUnionResolutionIsNotLossy:
    """A complete freshness row must NEVER resolve to the runtime-error branch.

    ``SourcesV3.results`` is ``list[Union[Results, Results1]]`` and ``Results`` (the
    runtime-error shape) requires only ``unique_id`` + ``status`` with ``extra="allow"``.
    If a full row resolved there, every freshness field would be dropped -- turning a
    loud parse failure into silent data loss, which is strictly worse.
    """

    @pytest.mark.parametrize(("fusion", "canonical"), FUSION_CASINGS)
    def test_full_row_resolves_to_output_branch(self, fusion, canonical):
        parsed = parse_sources(_sources_v3(fusion))
        (result,) = parsed.results
        assert isinstance(result, OutputV3)
        assert result.status.value == canonical
        assert result.max_loaded_at == "2026-08-19T08:33:50.855920Z"
        assert result.criteria.error_after.count == 48

    def test_runtime_error_row_still_resolves_to_runtime_error_branch(self):
        """dbt-core emits a distinct, field-less shape for a failed freshness check."""
        artifact = _sources_v3()
        artifact["results"] = [
            {
                "unique_id": "source.proj.sch.broken",
                "error": "Database Error: permission denied",
                "status": "runtime error",
            }
        ]
        (result,) = parse_sources(artifact).results
        assert isinstance(result, RuntimeErrorV3)
        assert result.status.value == "runtime error"


class TestFullArtifactParse:
    def test_fusion_artifact_parses_end_to_end(self):
        """The whole-file failure this regression is about: mixed Fusion statuses."""
        parsed = parse_sources(_sources_v3("Pass", "Error", "Warn", "Pass"))
        assert [r.status.value for r in parsed.results] == ["pass", "error", "warn", "pass"]
