from datetime import datetime
from typing import Dict, List, Optional, Text, Union

from pydantic.config import Extra
from pydantic.main import BaseModel


class AltimateCatalogMetadata(BaseModel):
    class Config:
        extra = Extra.forbid

    dbt_schema_version: Optional[Text] = "https://schemas.getdbt.com/dbt/catalog/v1.json"
    dbt_version: Optional[Text] = "0.19.0"
    generated_at: Optional[datetime] = "2021-02-10T04:42:33.680487Z"
    invocation_id: Optional[Optional[Text]] = None
    env: Optional[Dict[Text, Text]] = {}


class AltimateCatalogTableMetadata(BaseModel):
    class Config:
        extra = Extra.forbid

    type: Text
    database: Optional[Optional[Text]] = None
    schema_name: Text
    name: Text
    comment: Optional[Optional[Text]] = None
    owner: Optional[Optional[Text]] = None


class AltimateCatalogColumnMetadata(BaseModel):
    class Config:
        extra = Extra.forbid

    type: Text
    comment: Optional[Optional[Text]] = None
    index: int
    name: Text


class AltimateCatalogStatsItem(BaseModel):
    class Config:
        extra = Extra.forbid

    id: Text
    label: Text
    value: Optional[Optional[Union[bool, Text, float]]] = None
    description: Optional[Optional[Text]] = None
    include: bool


class AltimateCatalogTable(BaseModel):
    class Config:
        extra = Extra.forbid

    metadata: AltimateCatalogTableMetadata
    columns: Dict[Text, AltimateCatalogColumnMetadata]
    stats: Dict[Text, AltimateCatalogStatsItem]
    unique_id: Optional[Optional[Text]] = None


class AltimateCatalogCatalogV1(BaseModel):
    class Config:
        extra = Extra.forbid

    metadata: AltimateCatalogMetadata
    nodes: Dict[Text, AltimateCatalogTable]
    sources: Dict[Text, AltimateCatalogTable]
    errors: Optional[Optional[List[Text]]] = None
