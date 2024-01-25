from typing import Dict, Optional, Text

from datapilot.core.platforms.dbt.constants import FOLDER, MODEL
from datapilot.schemas.constants import (CONFIG_FOLDER_TYPE_PATTERNS,
                                         CONFIG_MODEL_TYPE_PATTERNS)


def get_regex_configuration(
    config: Optional[Dict],
) -> Dict[Text, Optional[Dict[Text, Text]]]:
    model_type_patterns = config.get(CONFIG_MODEL_TYPE_PATTERNS, None)
    folder_type_patterns = config.get(CONFIG_MODEL_TYPE_PATTERNS, None)
    return {
        MODEL: model_type_patterns,
        FOLDER: folder_type_patterns,
    }
    # Return the configured fanout threshold or the default if not specified
