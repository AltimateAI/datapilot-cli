from pydantic import BaseModel


class ModelNode(BaseModel):
    unique_id: str
    name: str
    resource_type: str
    database: str
    alias: str
    table_schema: str


class SourceNode(BaseModel):
    unique_id: str
    name: str
    resource_type: str
    table: str = ""
    database: str
    table_schema: str
