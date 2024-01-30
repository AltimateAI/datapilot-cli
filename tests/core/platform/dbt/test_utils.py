from pathlib import Path

import pytest

from datapilot.core.platforms.dbt.constants import BASE
from datapilot.core.platforms.dbt.constants import INTERMEDIATE
from datapilot.core.platforms.dbt.constants import MART
from datapilot.core.platforms.dbt.constants import OTHER
from datapilot.core.platforms.dbt.constants import STAGING
from datapilot.core.platforms.dbt.utils import MODEL_TYPE_PATTERNS
from datapilot.core.platforms.dbt.utils import _check_model_naming_convention
from datapilot.core.platforms.dbt.utils import classify_model_type_by_folder
from datapilot.core.platforms.dbt.utils import classify_model_type_by_name
from datapilot.core.platforms.dbt.utils import get_hard_coded_references


@pytest.mark.parametrize(
    ("sql_code", "expected"),
    [
        # Test with a simple FROM clause
        ("SELECT * FROM table1", set()),
        # Test with a JOIN clause
        ("SELECT * FROM table1 JOIN table2", set()),
        # Test with hard-coded references using {{ var() }}
        ("SELECT * FROM {{ var('schema.table') }}", {"{{ var('schema.table') }}"}),
        # Test with more complex SQL code
        (
            """
    SELECT *
    FROM schema.table1
    JOIN schema.table2 ON table1.id = table2.id
    WHERE table1.column = 'value'
    """,
            {"schema.table1", "schema.table2"},
        ),
        (
            """
        SELECT * FROM {{ ref('schema.table1') }} JOIN {{ ref('schema.table2') }} ON table1.id = table2.id
        """,
            set(),
        ),
        (
            """
        SELECT * FROM {{ ref('schema.table1') }} JOIN a.b ON table1.id = table2.id
        """,
            {"a.b"},
        ),
    ],
)
def test_get_hard_coded_references(sql_code, expected):
    assert get_hard_coded_references(sql_code) == expected


# Define the test cases
# Each test case includes: model_path, model_folder_pattern (optional), expected_output


@pytest.mark.parametrize(
    ("model_path", "model_folder_pattern", "expected"),
    [
        # Test cases without additional folder patterns
        (Path("path/to/staging/model_file.sql"), None, STAGING),
        (Path("path/to/mart/model_file.sql"), None, MART),
        (Path("path/to/intermediate/model_file.sql"), None, INTERMEDIATE),
        (Path("path/to/base/model_file.sql"), None, BASE),
        (Path("path/to/other/model_fil.sql"), None, OTHER),
        (Path("path/to/base/model_fil"), None, BASE),
        # Test cases with additional folder patterns
        (Path("path/to/custom/model_file.sql"), {"CUSTOM": "custom"}, "CUSTOM"),
        (Path("path/to/unknown/model_file.sql"), {"CUSTOM": "custom"}, OTHER),
        (Path("path/to/unknown/model_file"), {"CUSTOM": "custom"}, OTHER),
        # Override the default folder patterns
        (Path("path/to/staging/model_file.sql"), {STAGING: "^staging_.*"}, OTHER),
        (Path("path/to/staging_1/model_file.sql"), {STAGING: "^staging_.*"}, STAGING),
        (Path("path/to/staging_1/model_file.sql"), {STAGING: "^staging_.*", OTHER: "^other_.*"}, STAGING),
        (Path("path/to/other_1/model_file.sql"), {STAGING: "^staging_.*", OTHER: "^other_.*"}, OTHER),
    ],
)
def test_classify_model_type_by_folder(model_path, model_folder_pattern, expected):
    assert classify_model_type_by_folder(str(model_path), model_folder_pattern) == expected


@pytest.mark.parametrize(
    ("model_name", "model_name_pattern", "expected"),
    [
        # Test cases without additional patterns
        ("stg_example_model", None, STAGING),
        ("mrt_example_model", None, MART),
        ("mart_example_model", None, MART),
        ("fct_example_model", None, MART),
        ("dim_example_model", None, MART),
        ("int_example_model", None, INTERMEDIATE),
        ("base_example_model", None, BASE),
        ("unmatched_model", None, None),
        # Test cases with additional patterns
        ("custom_stg_model", {"CUSTOM": "^custom_.*"}, "CUSTOM"),
        ("custom_model", {"CUSTOM": "^custom_.*"}, "CUSTOM"),
        # Overriding the default pattern
        ("stg_example_model", {STAGING: "^staging_.*"}, None),
        ("staging_example_model", {STAGING: "^staging_.*"}, STAGING),
        # Overriding the default pattern with a custom pattern
        ("stg_example_model", {STAGING: "^custom_.*"}, None),
    ],
)
def test_classify_model_type_by_name(model_name, model_name_pattern, expected):
    assert classify_model_type_by_name(model_name, model_name_pattern) == expected


@pytest.mark.parametrize(
    ("model_name", "expected_model_type", "patterns", "expected"),
    [
        # Valid model names
        ("stg_model", STAGING, None, (True, None)),
        ("mrt_model", MART, None, (True, None)),
        ("int_model", INTERMEDIATE, None, (True, None)),
        ("base_model", BASE, None, (True, None)),
        # Invalid model names
        ("invalid_stg_model", STAGING, None, (False, MODEL_TYPE_PATTERNS[STAGING])),
        ("invalid_mrt_model", MART, None, (False, MODEL_TYPE_PATTERNS[MART])),
        (
            "invalid_int_model",
            INTERMEDIATE,
            None,
            (False, MODEL_TYPE_PATTERNS[INTERMEDIATE]),
        ),
        ("invalid_base_model", BASE, None, (False, MODEL_TYPE_PATTERNS[BASE])),
        # Test cases with additional patterns
        ("custom_model", "CUSTOM", {"CUSTOM": r"^custom_.*"}, (True, None)),
        (
            "invalid_custom_model",
            "CUSTOM",
            {"CUSTOM": r"^custom_.*"},
            (False, r"^custom_.*"),
        ),
    ],
)
def test_check_model_naming_convention(model_name, expected_model_type, patterns, expected):
    result = _check_model_naming_convention(model_name, expected_model_type, patterns)
    assert result == expected
