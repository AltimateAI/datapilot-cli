from typing import Dict
from typing import Optional

from datapilot.core.platforms.dbt.constants import FOLDER
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.schemas.constants import CONFIG_INSIGHTS
from datapilot.schemas.constants import CONFIG_MODEL_TYPE_PATTERNS


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


def get_insight_configuration(config: Optional[Dict]) -> Dict[str, Optional[Dict[str, str]]]:
    insight_config = config.get(CONFIG_INSIGHTS, None)
    return insight_config


def get_insight_config(config: Optional[Dict], insight_alias: str, config_key: str):
    if config is None:
        return None
    insights = config.get(CONFIG_INSIGHTS, {})
    insight = insights.get(insight_alias, {})
    return insight.get(config_key, None)
