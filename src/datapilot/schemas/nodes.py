from pydantic import BaseModel
from pydantic import ConfigDict


class ModelNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    unique_id: str
    name: str
    resource_type: str
    database: str
    alias: str
    table_schema: str


class SourceNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    unique_id: str
    name: str
    resource_type: str
    table: str = ""
    database: str
    table_schema: str
