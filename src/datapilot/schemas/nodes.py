from datapilot.schemas.base import AltimateBaseModel


class ModelNode(AltimateBaseModel):
    unique_id: str
    name: str
    resource_type: str
    database: str
    alias: str
    table_schema: str


class SourceNode(AltimateBaseModel):
    unique_id: str
    name: str
    resource_type: str
    table: str = ""
    database: str
    table_schema: str
