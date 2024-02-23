from typing import Dict
from typing import Optional

from datapilot.core.platforms.dbt.constants import FOLDER
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.schemas.constants import CONFIG_CONTRACT_PATTERNS
from datapilot.schemas.constants import CONFIG_DTYPES_PATTERNS
from datapilot.schemas.constants import CONFIG_LABELS_KEYS
from datapilot.schemas.constants import CONFIG_META_KEYS
from datapilot.schemas.constants import CONFIG_MODEL_TYPE_PATTERNS
from datapilot.schemas.constants import CONFIG_TEST_GROUP
from datapilot.schemas.constants import CONFIG_TEST_NAME
from datapilot.schemas.constants import CONFIG_TEST_TYPE


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
