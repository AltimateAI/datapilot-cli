import pytest

from datapilot.core.platforms.dbt.constants import (BASE, INTERMEDIATE, MART,
                                                    OTHER, STAGING)
from datapilot.core.platforms.dbt.utils import (MODEL_TYPE_PATTERNS,
                                                _check_model_naming_convention,
                                                classify_model_type_by_folder,
                                                classify_model_type_by_name,
                                                get_hard_coded_references)

test_cases = [
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
]


@pytest.mark.parametrize("sql_code,expected", test_cases)
def test_get_hard_coded_references(sql_code, expected):
    assert get_hard_coded_references(sql_code) == expected


# Define the test cases
# Each test case includes: model_path, model_folder_pattern (optional), expected_output
test_cases = [
    # Test cases without additional folder patterns
    ("/path/to/staging/model_file.sql", None, STAGING),
    ("/path/to/mart/model_file.sql", None, MART),
    ("/path/to/intermediate/model_file.sql", None, INTERMEDIATE),
    ("/path/to/base/model_file.sql", None, BASE),
    ("/path/to/other/model_fil.sql", None, OTHER),
    ("/path/to/base/model_fil", None, BASE),
    # Test cases with additional folder patterns
    ("/path/to/custom/model_file.sql", {"CUSTOM": "custom"}, "CUSTOM"),
    ("/path/to/unknown/model_file.sql", {"CUSTOM": "custom"}, OTHER),
    ("/path/to/unknown/model_file", {"CUSTOM": "custom"}, OTHER),
    # OVerride the default folder patterns
    ("/path/to/staging/model_file.sql", {STAGING: "^staging_.*"}, OTHER),
    ("/path/to/staging_1/model_file.sql", {STAGING: "^staging_.*"}, STAGING),
    ("/path/to/staging_1/model_file.sql", {STAGING: "^staging_.*"}, STAGING),
    (
        "/path/to/staging_1/model_file.sql",
        {STAGING: "^staging_.*", OTHER: "^other_.*"},
        STAGING,
    ),
    (
        "/path/to/other_1/model_file.sql",
        {STAGING: "^staging_.*", OTHER: "^other_.*"},
        OTHER,
    ),
]


@pytest.mark.parametrize("model_path, model_folder_pattern, expected", test_cases)
def test_classify_model_type_by_folder(model_path, model_folder_pattern, expected):
    assert classify_model_type_by_folder(model_path, model_folder_pattern) == expected


test_cases = [
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
]


@pytest.mark.parametrize("model_name, model_name_pattern, expected", test_cases)
def test_classify_model_type_by_name(model_name, model_name_pattern, expected):
    assert classify_model_type_by_name(model_name, model_name_pattern) == expected


test_cases = [
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
]


@pytest.mark.parametrize("model_name, expected_model_type, patterns, expected", test_cases)
def test_check_model_naming_convention(model_name, expected_model_type, patterns, expected):
    assert _check_model_naming_convention(model_name, expected_model_type, patterns) == expected
