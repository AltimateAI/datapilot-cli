from datetime import datetime
from typing import ClassVar
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1 as BaseCatalogV1
from dbt_artifacts_parser.parsers.catalog.catalog_v1 import Metadata as BaseMetadata
from pydantic.main import BaseModel


class AltimateCatalogMetadata(BaseModel):
    dbt_schema_version: Optional[str] = "https://schemas.getdbt.com/dbt/catalog/v1.json"
    dbt_version: Optional[str] = "0.19.0"
    generated_at: Optional[datetime] = "2021-02-10T04:42:33.680487Z"
    invocation_id: Optional[Optional[str]] = None
    env: ClassVar[Optional[Dict[str, str]]] = {}


class AltimateCatalogTableMetadata(BaseModel):
    type: str
    database: Optional[Optional[str]] = None
    schema_name: str
    name: str
    comment: Optional[Optional[str]] = None
    owner: Optional[Optional[str]] = None


class AltimateCatalogColumnMetadata(BaseModel):
    type: str
    comment: Optional[Optional[str]] = None
    index: int
    name: str


class AltimateCatalogStatsItem(BaseModel):
    id: str
    label: str
    value: Optional[Optional[Union[bool, str, float]]] = None
    description: Optional[Optional[str]] = None
    include: bool


class AltimateCatalogTable(BaseModel):
    metadata: AltimateCatalogTableMetadata
    columns: Dict[str, AltimateCatalogColumnMetadata]
    stats: Dict[str, AltimateCatalogStatsItem]
    unique_id: Optional[Optional[str]] = None


class AltimateCatalogCatalogV1(BaseModel):
    metadata: AltimateCatalogMetadata
    nodes: Dict[str, AltimateCatalogTable]
    sources: Dict[str, AltimateCatalogTable]
    errors: Optional[Optional[List[str]]] = None


# Custom classes to handle extra fields in newer dbt versions
class Metadata(BaseMetadata):
    class Config:
        extra = "allow"  # Allow extra fields in metadata


class CatalogV1(BaseCatalogV1):
    metadata: Metadata  # Use our custom metadata class

    class Config:
        extra = "allow"  # Allow extra fields


Catalog = CatalogV1
