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

    def get_columns(self):
        # Combine nodes and sources into a single dictionary for iteration
        combined_catalog_items = {**self.catalog.nodes, **self.catalog.sources}

        nodes_with_columns = {}
        for item_id, catalog_item in combined_catalog_items.items():
            # Initialize an empty list for each item to store its columns
            nodes_with_columns[item_id] = []
            for name, column_node in catalog_item.columns.items():
                # Append the column details to the corresponding item
                nodes_with_columns[item_id].append({"name": name, "data_type": column_node.type, "description": ""})
        return nodes_with_columns
