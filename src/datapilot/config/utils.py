from typing import Dict
from typing import Optional

from datapilot.core.platforms.dbt.constants import FOLDER
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.schemas.constants import CONFIG_BASE_FOLDER
from datapilot.schemas.constants import CONFIG_BLACKLIST_DATABASE
from datapilot.schemas.constants import CONFIG_BLACKLIST_SCHEMA
from datapilot.schemas.constants import CONFIG_CONTRACT_PATTERNS
from datapilot.schemas.constants import CONFIG_DTYPES_PATTERNS
from datapilot.schemas.constants import CONFIG_LABELS_KEYS
from datapilot.schemas.constants import CONFIG_MAX_CHILDS
from datapilot.schemas.constants import CONFIG_MAX_PARENTS
from datapilot.schemas.constants import CONFIG_META_KEYS
from datapilot.schemas.constants import CONFIG_MIN_CHILDS
from datapilot.schemas.constants import CONFIG_MIN_PARENTS
from datapilot.schemas.constants import CONFIG_MODEL_CONTRACT_PATTERNS
from datapilot.schemas.constants import CONFIG_MODEL_TYPE_PATTERNS
from datapilot.schemas.constants import CONFIG_SOURCE_LABELS_KEYS
from datapilot.schemas.constants import CONFIG_SOURCE_MAX_CHILDS
from datapilot.schemas.constants import CONFIG_SOURCE_META_KEYS
from datapilot.schemas.constants import CONFIG_SOURCE_MIN_CHILDS
from datapilot.schemas.constants import CONFIG_SOURCE_TAG_LIST
from datapilot.schemas.constants import CONFIG_SOURCE_TEST_COUNT
from datapilot.schemas.constants import CONFIG_SOURCE_TEST_GROUP
from datapilot.schemas.constants import CONFIG_SOURCE_TEST_NAME
from datapilot.schemas.constants import CONFIG_SOURCE_TEST_TYPE
from datapilot.schemas.constants import CONFIG_TAG_LIST
from datapilot.schemas.constants import CONFIG_TEST_GROUP
from datapilot.schemas.constants import CONFIG_TEST_NAME
from datapilot.schemas.constants import CONFIG_TEST_TYPE
from datapilot.schemas.constants import CONFIG_THRESHOLD_CHILDS
from datapilot.schemas.constants import CONFIG_WHITELIST_DATABASE
from datapilot.schemas.constants import CONFIG_WHITELIST_SCHEMA


def get_regex_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    model_type_patterns = config.get(CONFIG_MODEL_TYPE_PATTERNS, None)
    folder_type_patterns = config.get(CONFIG_MODEL_TYPE_PATTERNS, None)
    return {
        MODEL: model_type_patterns,
        FOLDER: folder_type_patterns,
    }
    # Return the configured fanout threshold or the default if not specified


def get_contract_regex_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_CONTRACT_PATTERNS, None)


def get_dtypes_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_DTYPES_PATTERNS, None)


def get_meta_keys_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_META_KEYS, None)


def get_labels_keys_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_LABELS_KEYS, None)


def get_test_name_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_TEST_NAME, None)


def get_test_type_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_TEST_TYPE, None)


def get_test_group_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_TEST_GROUP, None)


def get_model_regex_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_MODEL_CONTRACT_PATTERNS, None)


def get_whitelist_database_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_WHITELIST_DATABASE, None)


def get_blacklist_database_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_BLACKLIST_DATABASE, None)


def get_whitelist_schema_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_WHITELIST_SCHEMA, None)


def get_blacklist_schema_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_BLACKLIST_SCHEMA, None)


def get_tag_list_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_TAG_LIST, None)


def get_threshold_childs_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_THRESHOLD_CHILDS, None)


def get_source_meta_keys_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_META_KEYS, None)


def get_source_labels_keys_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_LABELS_KEYS, None)


def get_source_test_name_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_TEST_NAME, None)


def get_source_test_type_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_TEST_TYPE, None)


def get_source_test_group_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_TEST_GROUP, None)


def get_source_tag_list_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_TAG_LIST, None)


def get_source_min_childs_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_MIN_CHILDS, None)


def get_source_max_childs_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_MAX_CHILDS, None)


def get_source_test_count_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_SOURCE_TEST_COUNT, None)


def get_max_parents_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_MAX_PARENTS, None)


def get_min_parents_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_MIN_PARENTS, None)


def get_max_childs_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_MAX_CHILDS, None)


def get_min_childs_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_MIN_CHILDS, None)


def get_base_folder_configuration(
    config: Optional[Dict],
) -> Dict[str, Optional[Dict[str, str]]]:
    return config.get(CONFIG_BASE_FOLDER, None)
