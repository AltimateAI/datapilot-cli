from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1
from dbt_artifacts_parser.parsers.manifest.manifest_v1 import ManifestV1
from dbt_artifacts_parser.parsers.manifest.manifest_v2 import ManifestV2
from dbt_artifacts_parser.parsers.manifest.manifest_v3 import ManifestV3
from dbt_artifacts_parser.parsers.manifest.manifest_v4 import ManifestV4
from dbt_artifacts_parser.parsers.manifest.manifest_v5 import ManifestV5
from dbt_artifacts_parser.parsers.manifest.manifest_v6 import ManifestV6
from dbt_artifacts_parser.parsers.manifest.manifest_v7 import ManifestV7
from dbt_artifacts_parser.parsers.manifest.manifest_v8 import ManifestV8
from dbt_artifacts_parser.parsers.manifest.manifest_v9 import ManifestV9
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import ManifestV10
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import ManifestV11
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import SupportedLanguage
from pydantic import BaseModel
from pydantic import Extra


class DBTVersion(BaseModel):
    MAJOR: int
    MINOR: int
    PATCH: Optional[int]


Manifest = Union[
    ManifestV11,
    ManifestV10,
    ManifestV9,
    ManifestV8,
    ManifestV7,
    ManifestV6,
    ManifestV5,
    ManifestV4,
    ManifestV3,
    ManifestV2,
    ManifestV1,
]

Catalog = CatalogV1


class AltimateDocs(BaseModel):
    class Config:
        extra = Extra.forbid

    show: Optional[bool] = True
    node_color: Optional[Optional[str]] = None


class AltimateDependsOn(BaseModel):
    nodes: Optional[List[str]]
    macros: Optional[List[str]]


class AltimateManifestColumnInfo(BaseModel):
    name: str
    description: Optional[str] = ""
    meta: Optional[Dict[str, Any]] = {}
    data_type: Optional[Optional[str]] = None
    quote: Optional[Optional[bool]] = None
    tags: Optional[List[str]] = []


class AltimateFileHash(BaseModel):
    name: Optional[str]
    checksum: Optional[str]


class AltimateResourceType(Enum):
    source = "source"
    seed = "seed"
    snapshot = "snapshot"
    analysis = "analysis"
    model = "model"
    test = "test"
    doc = "doc"
    operation = "operation"
    macro = "macro"
    rpc = "rpc"
    single_run_operation = "single_run_operation"
    hook = "hook"
    sql_operation = "sql_operation"
    metric = "metric"
    exposure = "exposure"
    group = "group"


class AltimateAccess(Enum):
    private = "private"
    public = "public"
    protected = "protected"


class AltimateDBTContract(BaseModel):
    class Config:
        extra = Extra.forbid

    enforced: Optional[bool] = False
    alias_types: Optional[bool] = True
    checksum: Optional[Optional[str]] = None


class AltimateHook(BaseModel):
    class Config:
        extra = Extra.forbid

    sql: str
    transaction: Optional[bool] = True
    index: Optional[Optional[int]] = None


# TODO: Need to add the rest of the fields
class AltimateNodeConfig(BaseModel):
    class Config:
        extra = Extra.allow

    _extra: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = True
    alias: Optional[Optional[str]] = None
    schema_: Optional[Optional[str]] = None
    database: Optional[Optional[str]] = None
    tags: Optional[Union[List[str], str]] = None
    meta: Optional[Dict[str, Any]] = None
    group: Optional[Optional[str]] = None
    materialized: Optional[str] = "view"
    incremental_strategy: Optional[Optional[str]] = None
    persist_docs: Optional[Dict[str, Any]] = None
    post_hook: Optional[List[AltimateHook]]
    pre_hook: Optional[List[AltimateHook]]
    quoting: Optional[Dict[str, Any]] = None
    column_types: Optional[Dict[str, Any]] = None
    full_refresh: Optional[Optional[bool]] = None
    unique_key: Optional[Optional[Union[str, List[str]]]] = None
    on_schema_change: Optional[Optional[str]] = "ignore"


class AltimateManifestNode(BaseModel):
    database: Optional[str]
    resource_type: AltimateResourceType
    schema_name: str
    name: str
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    fqn: List[str]
    alias: str
    config: Optional[AltimateNodeConfig] = None
    raw_code: Optional[str] = ""
    language: Optional[str] = "sql"
    checksum: Optional[AltimateFileHash]
    description: Optional[str] = ""
    columns: Optional[Dict[str, AltimateManifestColumnInfo]] = None
    relation_name: Optional[Optional[str]] = None
    sources: Optional[List[List[str]]] = None
    metrics: Optional[List[List[str]]] = None
    depends_on: Optional[AltimateDependsOn] = None
    compiled_path: Optional[Optional[str]] = None
    compiled: Optional[bool] = False
    compiled_code: Optional[Optional[str]] = None
    access: Optional[AltimateAccess]
    contract: Optional[AltimateDBTContract] = None
    meta: Optional[Dict[str, Any]] = None
    patch_path: Optional[Optional[str]] = None


class AltimateQuoting(BaseModel):
    database: Optional[Optional[bool]] = None
    schema_: Optional[Optional[bool]] = None
    identifier: Optional[Optional[bool]] = None
    column: Optional[Optional[bool]] = None


class AltimateFreshnessThreshold(BaseModel):
    warn_after: Optional[Dict] = None
    error_after: Optional[Dict] = None
    filter: Optional[str] = None


class AltimateExternalPartition(BaseModel):
    name: Optional[str] = ""
    description: Optional[str] = ""
    data_type: Optional[str] = ""
    meta: Optional[Dict[str, Any]] = {}


class AltimateExternalTable(BaseModel):
    location: Optional[Optional[str]] = None
    file_format: Optional[Optional[str]] = None
    row_format: Optional[Optional[str]] = None
    tbl_properties: Optional[Optional[str]] = None
    partitions: Optional[Optional[List[AltimateExternalPartition]]] = None


class AltimateSourceConfig(BaseModel):
    enabled: Optional[bool] = True


class AltimateDeferRelation(BaseModel):
    class Config:
        extra = Extra.forbid

    database: Optional[str]
    schema_name: str
    alias: str
    relation_name: Optional[str]


class AltimateSeedConfig(BaseModel):
    class Config:
        extra = Extra.allow

    _extra: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = True
    alias: Optional[Optional[str]] = None
    schema_: Optional[Optional[str]] = None
    database: Optional[Optional[str]] = None
    tags: Optional[Union[List[str], str]] = None
    meta: Optional[Dict[str, Any]] = None
    group: Optional[Optional[str]] = None
    materialized: Optional[str] = "seed"
    incremental_strategy: Optional[Optional[str]] = None
    persist_docs: Optional[Dict[str, Any]] = None
    post_hook: Optional[List[AltimateHook]]
    pre_hook: Optional[List[AltimateHook]]
    quoting: Optional[Dict[str, Any]] = None
    column_types: Optional[Dict[str, Any]] = None
    full_refresh: Optional[Optional[bool]] = None
    unique_key: Optional[Optional[Union[str, List[str]]]] = None
    on_schema_change: Optional[Optional[str]] = "ignore"
    on_configuration_change: Optional[Any] = None
    grants: Optional[Dict[str, Any]] = None
    packages: Optional[List[str]] = None
    docs: Optional[AltimateDocs] = None
    contract: Optional[Any] = None
    delimiter: Optional[str] = ","
    quote_columns: Optional[Optional[bool]] = None


class AltimateSeedNode(BaseModel):
    database: Optional[str]
    schema_name: str
    name: str
    resource_type: AltimateResourceType
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    fqn: List[str]
    alias: str
    checksum: Optional[AltimateFileHash]
    config: Optional[AltimateSeedConfig] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = ""
    columns: Optional[Dict[str, AltimateManifestColumnInfo]] = None
    meta: Optional[Dict[str, Any]] = None
    group: Optional[Optional[str]] = None
    docs: Optional[Any] = None
    patch_path: Optional[Optional[str]] = None
    build_path: Optional[Optional[str]] = None
    deferred: Optional[bool] = False
    unrendered_config: Optional[Dict[str, Any]] = None
    created_at: Optional[float] = None
    config_call_dict: Optional[Dict[str, Any]] = None
    relation_name: Optional[Optional[str]] = None
    raw_code: Optional[str] = ""
    root_path: Optional[Optional[str]] = None
    depends_on: Optional[AltimateDependsOn] = None
    defer_relation: Optional[Optional[AltimateDeferRelation]] = None


class AltimateManifestSourceNode(BaseModel):
    database: Optional[str]
    resource_type: AltimateResourceType
    schema_name: str
    name: str
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    fqn: List[str]
    source_name: str
    source_description: str
    loader: str
    identifier: str
    quoting: Optional[AltimateQuoting] = None
    loaded_at_field: Optional[Optional[str]] = None
    freshness: Optional[Optional[AltimateFreshnessThreshold]] = None
    external: Optional[Optional[AltimateExternalTable]] = None
    description: Optional[str] = ""
    columns: Optional[Dict[str, AltimateManifestColumnInfo]] = None
    meta: Optional[Dict[str, Any]] = None
    relation_name: Optional[Optional[str]] = None
    source_meta: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    config: Optional[AltimateSourceConfig] = None
    patch_path: Optional[Optional[str]] = None
    unrendered_config: Optional[Dict[str, Any]] = None
    created_at: Optional[float] = None


class AltimateExposureType(Enum):
    dashboard = "dashboard"
    notebook = "notebook"
    analysis = "analysis"
    ml = "ml"
    application = "application"


class AltimateOwner(BaseModel):
    class Config:
        extra = Extra.allow

    _extra: Optional[Dict[str, Any]] = None
    email: Optional[Optional[str]] = None
    name: Optional[Optional[str]] = None


class AltimateMaturityEnum(Enum):
    low = "low"
    medium = "medium"
    high = "high"


class AltimateRefArgs(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    package: Optional[Optional[str]] = None
    version: Optional[Optional[Union[str, float]]] = None


class AltimateExposureConfig(BaseModel):
    class Config:
        extra = Extra.allow

    _extra: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = True


class AltimateManifestExposureNode(BaseModel):
    name: str
    resource_type: AltimateResourceType
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    fqn: List[str]
    type: AltimateExposureType
    owner: AltimateOwner
    description: Optional[str] = ""
    label: Optional[Optional[str]] = None
    maturity: Optional[Optional[AltimateMaturityEnum]] = None
    meta: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    config: Optional[AltimateExposureConfig] = None
    unrendered_config: Optional[Dict[str, Any]] = None
    url: Optional[Optional[str]] = None
    depends_on: Optional[AltimateDependsOn] = None
    refs: Optional[List[AltimateRefArgs]] = None
    sources: Optional[List[List[str]]] = None
    metrics: Optional[List[List[str]]] = None
    created_at: Optional[float] = None


class AltimateTestMetadata(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    kwargs: Optional[Dict[str, Any]] = None
    namespace: Optional[Optional[str]] = None


class AltimateTestConfig(BaseModel):
    class Config:
        extra = Extra.allow

    _extra: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = True
    alias: Optional[Optional[str]] = None
    schema_: Optional[Optional[str]] = None
    database: Optional[Optional[str]] = None
    tags: Optional[Union[List[str], str]] = None
    meta: Optional[Dict[str, Any]] = None
    group: Optional[Optional[str]] = None
    materialized: Optional[str] = "test"
    severity: Optional[str] = "ERROR"
    store_failures: Optional[Optional[bool]] = None
    store_failures_as: Optional[Optional[str]] = None
    where: Optional[Optional[str]] = None
    limit: Optional[Optional[int]] = None
    fail_calc: Optional[str] = "count(*)"
    warn_if: Optional[str] = "!= 0"
    error_if: Optional[str] = "!= 0"


class AltimateManifestTestNode(BaseModel):
    test_metadata: Optional[AltimateTestMetadata] = None
    test_type: Optional[str] = None
    name: str
    resource_type: AltimateResourceType
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    fqn: List[str]
    alias: str
    checksum: Optional[AltimateFileHash]
    config: Optional[AltimateTestConfig] = None
    _event_status: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = ""
    columns: Optional[Dict[str, AltimateManifestColumnInfo]] = None
    meta: Optional[Dict[str, Any]] = None
    group: Optional[Optional[str]] = None
    raw_code: Optional[str] = ""
    language: Optional[str] = "sql"
    refs: Optional[List[AltimateRefArgs]] = None
    sources: Optional[List[List[str]]] = None
    metrics: Optional[List[List[str]]] = None
    depends_on: Optional[AltimateDependsOn] = None
    compiled_path: Optional[Optional[str]] = None
    compiled: Optional[bool] = False
    compiled_code: Optional[Optional[str]] = None


class AltimateMacroArgument(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str
    type: Optional[Optional[str]] = None
    description: Optional[Optional[str]] = ""


AltimateSupportedLanguage = SupportedLanguage


class AltimateManifestMacroNode(BaseModel):
    name: str
    resource_type: AltimateResourceType
    package_name: str
    path: str
    original_file_path: str
    unique_id: str
    macro_sql: str
    depends_on: Optional[AltimateDependsOn] = None
    description: Optional[str] = ""
    meta: Optional[Dict[str, Any]] = None
    docs: Optional[AltimateDocs] = None
    patch_path: Optional[Optional[str]] = None
    arguments: Optional[List[AltimateMacroArgument]] = None
    created_at: Optional[float] = None
    supported_languages: Optional[Optional[List[AltimateSupportedLanguage]]] = None
