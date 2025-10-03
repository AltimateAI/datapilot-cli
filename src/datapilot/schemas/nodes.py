from dataclasses import dataclass


@dataclass
class ModelNode:
    unique_id: str
    name: str
    resource_type: str
    database: str
    alias: str
    table_schema: str


@dataclass
class SourceNode:
    unique_id: str
    name: str
    resource_type: str
    database: str
    table_schema: str
    table: str = ""
