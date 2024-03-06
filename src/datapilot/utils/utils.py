import json
import os
import re
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Dict

from datapilot.schemas.nodes import ModelNode
from datapilot.schemas.nodes import SourceNode


def load_json(file_path: str) -> Dict:
    try:
        with Path(file_path).open() as f:
            return json.load(f)
    except FileNotFoundError:
        raise
    except json.decoder.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON file: {file_path}") from e
    except IsADirectoryError as e:
        raise ValueError(f"Please provide a A valid manifest file path. {file_path} is a directory") from e


def extract_dir_name_from_file_path(path: str) -> str:
    # Handle both Windows and Linux paths using os.path
    # Get root directory name
    return Path(path).parent.name


def extract_folders_in_path(path: str) -> list:
    # Split the path into parts
    path_parts = path.split(os.path.sep)

    # Exclude the last part if it's a file (has a file extension)
    if "." in path_parts[-1]:
        path_parts = path_parts[:-1]
    path_parts = [part for part in path_parts if part != ""]
    return path_parts


def get_dir_path(path: str) -> str:
    """
    Get the directory path of a file path.
    For example, if the path is /a/b/c/d.txt, the directory path is /a/b/c

    :param path:
    :return:
    """
    return Path(path).parent


def is_superset_path(superset_path: str, path: str):
    """
    Check if the path is a sub-path of the superset path.

    :param superset_path: The superset path
    :param path: The path to be checked
    :return: True if the path is a sub-path of the superset path, False otherwise
    """

    try:
        Path(path).relative_to(superset_path)
        return True
    except ValueError:
        return False


def get_changed_files(include_untracked=True):
    command = ["git", "status", "--porcelain"]
    if include_untracked:
        command.append("-uall")
    result = subprocess.run(command, capture_output=True, text=True)  # noqa
    changed_files = []
    for line in result.stdout.splitlines():
        if line.startswith("??") and include_untracked:
            changed_files.append(line.split()[1])
        elif line.startswith(("M", "A", "D", "R", " M", " A", " D", " R")):
            changed_files.append(line.split()[1])
    return changed_files


def get_tmp_dir_path():
    tmp_dir = Path(tempfile.gettempdir()) / str(uuid.uuid4())
    tmp_dir.mkdir(parents=True, exist_ok=True)
    return tmp_dir


def get_column_type(dtype: str) -> str:
    dtype = dtype.lower()
    if re.match(r".*int.*", dtype):
        return "INTEGER"
    elif re.match(r".*float.*", dtype):
        return "FLOAT"
    elif re.match(r".*bool.*", dtype):
        return "BOOLEAN"
    elif re.match(r".*date.*", dtype):
        return "DATE"
    elif re.match(r".*time.*", dtype):
        return "TIME"
    elif re.match(r".*timestamp.*", dtype):
        return "TIMESTAMP"
    elif re.match(r".*text.*", dtype):
        return "TEXT"
    elif re.match(r".*char.*", dtype):
        return "TEXT"
    elif re.match(r".*varchar.*", dtype):
        return "TEXT"
    elif re.match(r".*numeric.*", dtype):
        return "NUMERIC"
    elif re.match(r".*decimal.*", dtype):
        return "DECIMAL"
    elif re.match(r".*double.*", dtype):
        return "DOUBLE"
    elif re.match(r".*real.*", dtype):
        return "REAL"
    else:
        return "TEXT"


def get_manifest_model_nodes(manifest: Dict, models: list) -> list[ModelNode]:
    nodes = []
    for node in manifest["nodes"].values():
        if node["name"] in models:
            if node["resource_type"] == "model":
                nodes.append(
                    ModelNode(
                        unique_id=node["unique_id"],
                        name=node["name"],
                        resource_type=node["resource_type"],
                        database=node["database"],
                        alias=node["alias"],
                        table_schema=node["schema"],
                    )
                )
    return nodes


def get_manifest_source_nodes(manifest: Dict) -> list[SourceNode]:
    nodes = []
    for node in manifest["sources"].values():
        nodes.append(
            SourceNode(
                unique_id=node["unique_id"],
                name=node["name"],
                resource_type=node["resource_type"],
                table=node["identifier"],
                database=node["database"],
                table_schema=node["schema"],
            )
        )
    return nodes


def get_model_tables(models: list[ModelNode]) -> list[str]:
    tables = []
    for model in models:
        tables.append(f"{model.database}.{model.table_schema}.{model.alias}")
    return tables


def get_source_tables(sources: list[SourceNode]) -> list[str]:
    tables = []
    for source in sources:
        tables.append(f"{source.database}.{source.table_schema}.{source.name}")
    return tables


def get_table_name(node: ModelNode | SourceNode, node_type: str) -> str:
    if node_type == "nodes":
        return f"{node.database}.{node.table_schema}.{node.alias}"
    return f"{node.database}.{node.table_schema}.{node.name}"


def fill_catalog(table_columns_map: Dict, manifest: Dict, catalog: Dict, nodes: list[ModelNode | SourceNode], node_type: str) -> Dict:
    for node in nodes:
        columns = {}
        for column in table_columns_map[get_table_name(node, node_type)]:
            column_type = get_column_type(column["type"])
            columns[column["name"]] = {
                "type": column_type,
                "index": len(columns) + 1,
                "name": column["name"],
                "comment": None,
            }

        catalog[node_type] = {
            node.unique_id: {
                "metadata": {
                    "type": "BASE TABLE",
                    "schema": manifest[node_type][node.unique_id]["schema"],
                    "name": node.alias if node_type == "nodes" else node.name,
                    "database": manifest[node_type][node.unique_id]["database"],
                    "comment": None,
                    "owner": None,
                },
                "columns": columns,
                "stats": {},
                "unique_id": node.unique_id,
            }
        }

    return catalog


def run_macro(macro: str) -> str:
    dbt_compile = subprocess.run(
        ["dbt", "compile", "--inline", macro],  # noqa
        capture_output=True,
        text=True,
    )
    return dbt_compile.stdout


def generate_partial_manifest_catalog(changed_files, manifest_path: str, catalog_path: str):
    try:
        models = [Path(f).stem for f in changed_files]

        subprocess.run(["dbt", "parse"])  # noqa

        manifest_file = Path("target/manifest.json")
        with manifest_file.open() as f:
            manifest = json.load(f)

        nodes = get_manifest_model_nodes(manifest, models)
        sources = get_manifest_source_nodes(manifest)

        nodes_tables = get_model_tables(nodes)
        sources_tables = get_source_tables(sources)
        tables = nodes_tables + sources_tables

        query = (
            """
        {% set maximum = 10000 %}
        {% set columns_list = [] %}
        {% for table in """
            + str(tables)
            + """ %}
        {%- set sql -%}
            describe table {{ table }}
        {%- endset -%}
        {%- set result = run_query(sql) -%}

        {% if (result | length) >= maximum %}
            {% set msg %}
            Too many columns in relation {{ table }}! dbt can only get
            information about relations with fewer than {{ maximum }} columns.
            {% endset %}
            {% do exceptions.raise_compiler_error(msg) %}
        {% endif %}

        {% for row in result %}
            {% do columns_list.append({'table': table, 'name': row['name'], 'type': row['type']}) %}
        {% endfor %}
        {% endfor %}
        {{ tojson(columns_list) }}
        """
        )

        dbt_compile_output = run_macro(query)

        print(dbt_compile_output)

        compiled_inline_node = dbt_compile_output.split("Compiled inline node is:")[1].strip().replace("'", "").strip()

        compiled_dict = json.loads(compiled_inline_node)

        # we need to get all columns  from compiled_dict which is a list of dictionaries
        # and each item in the list is a dictionary with keys table, name, type
        # we need to create a map of all the columns for each table
        # and then create a catalog for each table

        table_columns_map = {}
        for column in compiled_dict:
            if column["table"] in table_columns_map:
                table_columns_map[column["table"]].append(column)
            else:
                table_columns_map[column["table"]] = [column]

        catalog = {
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
                "dbt_version": "1.7.2",
                "generated_at": "2024-03-04T11:13:52.284167Z",
                "invocation_id": "e2970ef7-c397-404b-ac5d-63a71a45b628",
                "env": {},
            },
            "errors": None,
        }

        catalog = fill_catalog(table_columns_map, manifest, catalog, nodes, "nodes")
        catalog = fill_catalog(table_columns_map, manifest, catalog, sources, "sources")

        with Path.open(manifest_path, "w") as f:
            json.dump(manifest, f)
        with Path.open(catalog_path, "w") as f:
            print(catalog_path)
            json.dump(catalog, f)
    except Exception as e:
        raise Exception("Unable to generate partial manifest and catalog") from e


if __name__ == "__main__":
    print("Running main")
    print(generate_partial_manifest_catalog([], "/Users/gaurp/Desktop/manifest.json", ""))
    print("Done running main")
