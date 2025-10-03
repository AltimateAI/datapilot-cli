from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from vendor.dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1


@dataclass
class AltimateCatalogMetadata:
    dbt_schema_version: Optional[str] = "https://schemas.getdbt.com/dbt/catalog/v1.json"
    dbt_version: Optional[str] = "0.19.0"
    generated_at: Optional[datetime] = "2021-02-10T04:42:33.680487Z"
    invocation_id: Optional[Optional[str]] = None
    env: ClassVar[Optional[Dict[str, str]]] = {}


@dataclass
class AltimateCatalogTableMetadata:
    type: str
    schema_name: str
    name: str
    database: Optional[Optional[str]] = None
    comment: Optional[Optional[str]] = None
    owner: Optional[Optional[str]] = None


@dataclass
class AltimateCatalogColumnMetadata:
    type: str
    index: int
    name: str
    comment: Optional[Optional[str]] = None


@dataclass
class AltimateCatalogStatsItem:
    id: str
    label: str
    include: bool
    value: Optional[Optional[Union[bool, str, float]]] = None
    description: Optional[Optional[str]] = None


@dataclass
class AltimateCatalogTable:
    metadata: AltimateCatalogTableMetadata
    columns: Dict[str, AltimateCatalogColumnMetadata]
    stats: Dict[str, AltimateCatalogStatsItem]
    unique_id: Optional[Optional[str]] = None


@dataclass
class AltimateCatalogCatalogV1:
    metadata: AltimateCatalogMetadata
    nodes: Dict[str, AltimateCatalogTable]
    sources: Dict[str, AltimateCatalogTable]
    errors: Optional[Optional[List[str]]] = None


Catalog = CatalogV1
