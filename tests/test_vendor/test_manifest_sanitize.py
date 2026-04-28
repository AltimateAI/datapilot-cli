"""Tests for manifest parser fallback that sanitizes unused-but-required fields."""
import copy
import json
from pathlib import Path

import pytest

from vendor.dbt_artifacts_parser.parser import parse_manifest
from vendor.dbt_artifacts_parser.parsers.manifest.manifest_v12 import ManifestV12

FIXTURE_PATH = Path(__file__).parent.parent / "data" / "manifest_v12.json"


@pytest.fixture()
def valid_manifest() -> dict:
    with FIXTURE_PATH.open() as f:
        return json.load(f)


class TestManifestSanitize:
    def test_valid_manifest_parses(self, valid_manifest):
        manifest = parse_manifest(valid_manifest)
        assert isinstance(manifest, ManifestV12)

    def test_docs_entry_missing_resource_type_is_sanitized(self, valid_manifest):
        broken = copy.deepcopy(valid_manifest)
        del broken["docs"]["doc.dbt.__overview__"]["resource_type"]

        manifest = parse_manifest(broken)

        assert isinstance(manifest, ManifestV12)
        assert manifest.docs == {}

    def test_disabled_field_missing_is_sanitized(self, valid_manifest):
        broken = copy.deepcopy(valid_manifest)
        del broken["disabled"]

        manifest = parse_manifest(broken)

        assert isinstance(manifest, ManifestV12)
        assert manifest.disabled == {}

    def test_both_unused_fields_broken_is_sanitized(self, valid_manifest):
        broken = copy.deepcopy(valid_manifest)
        del broken["docs"]["doc.dbt.__overview__"]["resource_type"]
        del broken["disabled"]

        manifest = parse_manifest(broken)

        assert isinstance(manifest, ManifestV12)
        assert manifest.docs == {}
        assert manifest.disabled == {}
