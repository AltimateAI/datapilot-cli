"""Tests for converting project-owned macro `supported_languages` in the manifest wrappers.

`AltimateManifestMacroNode.supported_languages` is typed with the v12 `SupportedLanguage`
enum. The v10/v11 wrappers passed their own vendored enum members through unchanged, and
Pydantic v2 rejects members of a different Enum class even when the value matches. So
`get_macros()`, which `DBTInsightGenerator` always calls, failed for any v10/v11 project
with a custom materialization.
"""
import json
from pathlib import Path

import pytest

from datapilot.core.platforms.dbt.factory import DBTFactory
from vendor.dbt_artifacts_parser.parser import parse_manifest

DATA = Path(__file__).parent.parent.parent.parent / "data"


@pytest.mark.parametrize("fixture", ["manifest_v10.json", "manifest_v11.json"])
def test_wraps_project_macro_with_supported_languages(fixture):
    """GIVEN a v10/v11 manifest whose project owns a macro declaring `supported_languages`
    WHEN the parsed manifest is wrapped for insights
    THEN the languages convert by value without a ValidationError."""
    with (DATA / fixture).open() as f:
        manifest = json.load(f)
    macro_id = next(k for k, m in manifest["macros"].items() if m.get("supported_languages"))
    manifest["macros"][macro_id]["package_name"] = manifest["metadata"]["project_name"]
    manifest["macros"][macro_id]["supported_languages"] = ["sql"]

    macros = DBTFactory.get_manifest_wrapper(parse_manifest(manifest)).get_macros()

    assert [lang.value for lang in macros[macro_id].supported_languages] == ["sql"]
