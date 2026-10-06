"""Tests for manifest v12 parsing against manifests produced by dbt-core 1.11+.

dbt-core 1.11 added the built-in ``materialization_function_default`` macro, whose
``supported_languages`` include ``"javascript"``. The v12 ``SupportedLanguage`` enum
only allowed ``python``/``sql``, so EVERY manifest from dbt-core 1.11+ failed to parse
and project governance broke for all users on current dbt.

The failure was masked by the ``disabled`` fallback: it dropped ``disabled`` from the
manifest, but v12 declares that field required (nullable), so the retry always raised
``disabled: Field required`` instead of the real error.
"""
import copy
import json
from pathlib import Path

import pytest

from datapilot.core.platforms.dbt.factory import DBTFactory
from vendor.dbt_artifacts_parser.parser import _try_parse_manifest
from vendor.dbt_artifacts_parser.parser import parse_manifest
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v12 import ManifestV12

MANIFEST_V12 = Path(__file__).parent.parent / "data" / "manifest_v12.json"
FUNCTION_MACRO_ID = "macro.dbt.materialization_function_default"


@pytest.fixture
def manifest() -> dict:
    with MANIFEST_V12.open() as f:
        return json.load(f)


def _with_function_macro(manifest: dict) -> dict:
    """Add the macro dbt-core 1.11+ ships, cloned from an existing macro in the fixture."""
    manifest = copy.deepcopy(manifest)
    macro = copy.deepcopy(next(iter(manifest["macros"].values())))
    macro.update(
        unique_id=FUNCTION_MACRO_ID,
        name="materialization_function_default",
        supported_languages=["sql", "python", "javascript"],
    )
    manifest["macros"][FUNCTION_MACRO_ID] = macro
    return manifest


def test_parses_javascript_supported_language(manifest):
    """GIVEN a dbt-core 1.11+ manifest containing a macro that supports JavaScript
    WHEN it is parsed
    THEN parsing succeeds and the language is preserved."""
    parsed = parse_manifest(_with_function_macro(manifest))

    assert isinstance(parsed, ManifestV12)
    languages = [lang.value for lang in parsed.macros[FUNCTION_MACRO_ID].supported_languages]
    assert languages == ["sql", "python", "javascript"]


def test_wraps_project_macro_supporting_javascript(manifest):
    """GIVEN a project-owned macro (e.g. a custom UDF materialization) supporting JavaScript
    WHEN the parsed manifest is wrapped for insights
    THEN the macro's languages convert without a ValueError."""
    manifest = _with_function_macro(manifest)
    project = manifest["metadata"]["project_name"]
    manifest["macros"][FUNCTION_MACRO_ID]["package_name"] = project

    macros = DBTFactory.get_manifest_wrapper(parse_manifest(manifest)).get_macros()

    languages = [lang.value for lang in macros[FUNCTION_MACRO_ID].supported_languages]
    assert languages == ["sql", "python", "javascript"]


def test_unparseable_disabled_falls_back_to_none(manifest):
    """GIVEN a manifest whose ``disabled`` section does not match the strict schema
    WHEN it is parsed
    THEN the fallback nulls ``disabled`` (required in v12) and parsing succeeds."""
    manifest["disabled"] = {"model.proj.broken": [{"resource_type": "not-a-real-type"}]}

    parsed = parse_manifest(manifest)

    assert isinstance(parsed, ManifestV12)
    assert parsed.disabled is None


def test_failed_fallback_raises_the_original_error():
    """GIVEN a manifest whose first parse and `disabled`-nulled retry fail with different errors
    WHEN it is parsed
    THEN the original error is raised, not the retry's."""

    class Model:
        def __init__(self, **manifest):
            if manifest["disabled"] is None:
                raise ValueError("retry error")
            raise ValueError("original error")

    with pytest.raises(ValueError, match="^original error$"):
        _try_parse_manifest({"disabled": {}}, Model)
