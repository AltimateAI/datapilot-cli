from pathlib import Path

import pytest

from datapilot.core.platforms.dbt.utils import get_manifest_wrapper
from datapilot.core.platforms.dbt.utils import get_models
from datapilot.utils.utils import extract_folders_in_path
from datapilot.utils.utils import is_superset_path

test_cases = [
    (Path("/home/user/documents/file.txt"), ["home", "user", "documents"]),
    (Path("/home/user/documents/"), ["home", "user", "documents"]),
    (Path("/home/user/documents"), ["home", "user", "documents"]),
    (Path(), []),
    (Path("/"), []),
    (Path("file.txt"), []),
]


@pytest.mark.parametrize(("input_path", "expected"), test_cases)
def test_extract_folders_in_path(input_path, expected):
    assert extract_folders_in_path(str(input_path)) == expected


test_cases = [
    (["model1", "model2"], []),
    (["customers"], ["model.jaffle_shop_package.customers", "source.jaffle_shop_package.jaffle_shop.customers"]),
    (
        ["path:models/staging"],
        ["model.jaffle_shop_package.stg_customers", "model.jaffle_shop_package.stg_orders", "model.jaffle_shop_package.stg_payments"],
    ),
    (
        ["path:models/staging/stg_customers.sql"],
        ["model.jaffle_shop_package.stg_customers"],
    ),
]


@pytest.mark.parametrize(("selected_model_list", "expected"), test_cases)
def test_model_selections(selected_model_list, expected):
    manifest_wrapper = get_manifest_wrapper("tests/data/manifest_v11.json")
    nodes = manifest_wrapper.get_nodes()
    sources = manifest_wrapper.get_sources()
    exposures = manifest_wrapper.get_exposures()
    tests = manifest_wrapper.get_tests()
    entities = {
        "nodes": nodes,
        "sources": sources,
        "exposures": exposures,
        "tests": tests,
    }
    selected_models = [model for model in get_models(selected_model_list, entities) if not model.startswith("test.")]

    assert sorted(selected_models) == sorted(expected)


@pytest.mark.parametrize(
    ("superset_path", "path", "expected"),
    [
        ("/home/user", "/home/user/documents", True),
        ("/home/user/documents", "/home/user2/documents", False),
        ("/home/user", "/home/user", True),
        (".", "./subdirectory", True),
        ("/home/user/documents", "/home/user/../user2", False),
        ("/home/user", "/home/user/docs", True),
        ("/home/user", "/home/user/docs/../docs", True),
    ],
)
def test_is_superset_path(superset_path, path, expected):
    assert is_superset_path(superset_path, path) == expected
