from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1

from datapilot.core.platforms.dbt.wrappers.catalog.wrapper import BaseCatalogWrapper


class CatalogV1Wrapper(BaseCatalogWrapper):
    def __init__(self, catalog: CatalogV1):
        self.catalog = catalog

    def get_schema(self):
        nodes_with_schemas = {}
        for node_id, catalog_table_node in self.catalog.nodes.items():
            nodes_with_schemas[node_id] = {column_name: column_node.type for column_name, column_node in catalog_table_node.columns.items()}
        for source_id, catalog_source_node in self.catalog.sources.items():
            nodes_with_schemas[source_id] = {
                column_name: column_node.type for column_name, column_node in catalog_source_node.columns.items()
            }
        return nodes_with_schemas
