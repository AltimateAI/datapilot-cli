SEED = "seed"
MACRO = "macro"
TEST = "test"
MODEL = "model"
SOURCE = "source"

LLM = "llm"


PROJECT = "project"
SQL = "sql"

# Model Types
MART: str = "mart"
STAGING = "staging"
INTERMEDIATE = "intermediate"
BASE = "base"
OTHER = "other"

# MATERIALIZATION
TABLE = "table"
INCREMENTAL = "incremental"
VIEW = "view"
EPHEMERAL = "ephemeral"


MATERIALIZED = [TABLE, INCREMENTAL]
NON_MATERIALIZED = [VIEW, EPHEMERAL]


GENERIC = "generic"
SINGULAR = "singular"
OTHER_TEST_NODE = "other_test_node"


FOLDER = "folder"
