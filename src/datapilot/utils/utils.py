import json
import os
import re
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Dict
from typing import List
from typing import Union

from dbt_artifacts_parser.parser import parse_catalog
from dbt_artifacts_parser.parser import parse_manifest

from datapilot.config.config import load_config
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


def get_manifest_model_nodes(manifest: Dict, models: List) -> List[ModelNode]:
    nodes = []
    for node in manifest["nodes"].values():
        if node["name"] in models:
            if node["resource_type"] == "model" and node["config"]["materialized"] in ["table", "view"]:
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


def get_manifest_source_nodes(manifest: Dict, sources: List) -> List[SourceNode]:
    nodes = []
    for node in manifest["sources"].values():
        if node["source_name"] in sources:
            nodes.append(
                SourceNode(
                    unique_id=node["unique_id"],
                    name=node["source_name"],
                    resource_type=node["resource_type"],
                    table=node["identifier"],
                    database=node["database"],
                    table_schema=node["schema"],
                )
            )
    return nodes


def get_model_tables(models: List[ModelNode]) -> List[str]:
    tables = []
    for model in models:
        tables.append(f"{model.database}.{model.table_schema}.{model.alias}")
    return tables


def get_source_tables(sources: List[SourceNode]) -> List[str]:
    tables = []
    for source in sources:
        tables.append(f"{source.database}.{source.table_schema}.{source.name}")
    return tables


def get_table_name(node: Union[ModelNode, SourceNode], node_type: str) -> str:
    if node_type == "nodes":
        return f"{node.database}.{node.table_schema}.{node.alias}"
    return f"{node.database}.{node.table_schema}.{node.name}"


def fill_catalog(table_columns_map: Dict, manifest: Dict, catalog: Dict, nodes: List[Union[ModelNode, SourceNode]], node_type: str) -> Dict:
    if not nodes:
        catalog[node_type] = {}
        return catalog

    for node in nodes:
        columns = {}
        for column in table_columns_map[node.unique_id]:
            column_type = get_column_type(column["dtype"])
            columns[column["column"]] = {
                "type": column_type,
                "index": len(columns) + 1,
                "name": column["column"],
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


def run_macro(macro: str, base_path: str) -> str:
    dbt_compile = subprocess.run(
        ["dbt", "compile", "--inline", macro],  # noqa
        capture_output=True,
        cwd=base_path,
        text=True,
    )
    return dbt_compile.stdout


def generate_partial_manifest_catalog(changed_files, base_path: str = "./"):
    try:
        # print(f"Running generate_partial_manifest_catalog for {changed_files}")
        yaml_files = [
            f for f in changed_files if Path(f).suffix in [".yml", ".yaml"] and Path(f).name not in ["dbt_project.yml", "profiles.yml"]
        ]
        model_stem = [Path(f).stem for f in changed_files if Path(f).suffix in [".sql"]]
        # print(f"yaml_files: {yaml_files}")
        # print(f"model_stem: {model_stem}")
        model_set = set()
        source_set = set()

        for file in yaml_files:
            parsed_file = load_config(file)
            if "models" in parsed_file:
                for model in parsed_file["models"]:
                    model_set.add(model.get("name", ""))
            if "sources" in parsed_file:
                for source in parsed_file["sources"]:
                    source_set.add(source.get("name", ""))

        for model in model_stem:
            model_set.add(model)

        models = list(model_set)
        source_list = list(source_set)

        # print(f"models: {models}")
        # print(f"sources: {source_list}")
        subprocess.run(["dbt", "parse"], cwd=base_path, stdout=subprocess.PIPE)  # noqa

        manifest_file = Path(Path(base_path) / "target/manifest.json")
        with manifest_file.open() as f:
            manifest = json.load(f)

        nodes = get_manifest_model_nodes(manifest, models)
        sources = get_manifest_source_nodes(manifest, source_list)

        nodes_data = [{"name": node.name, "resource_type": node.resource_type, "unique_id": node.unique_id, "table": ""} for node in nodes]

        sources_data = [
            {"name": source.name, "resource_type": source.resource_type, "unique_id": source.unique_id, "table": source.table}
            for source in sources
        ]

        nodes_str = ",\n".join(json.dumps(data) for data in nodes_data + sources_data)

        query = (
            "{% set result = {} %}{% set nodes = ["
            + nodes_str
            + '] %}{% for n in nodes %}{% if n["resource_type"] == "source" %}{% set columns = adapter.get_columns_in_relation(source(n["name"], n["table"])) %}{% else %}{% set columns = adapter.get_columns_in_relation(ref(n["name"])) %}{% endif %}{% set new_columns = [] %}{% for column in columns %}{% do new_columns.append({"column": column.name, "dtype": column.dtype}) %}{% endfor %}{% do result.update({n["unique_id"]:new_columns}) %}{% endfor %}{{ tojson(result) }}'
        )

        dbt_compile_output = run_macro(query, base_path)

        # print(dbt_compile_output)

        compiled_inline_node = dbt_compile_output.split("Compiled inline node is:")[1].strip().replace("'", "").strip()

        table_columns_map = json.loads(compiled_inline_node)

        # we need to get all columns  from compiled_dict which is a list of dictionaries
        # and each item in the list is a dictionary with keys table, name, type
        # we need to create a map of all the columns for each table
        # and then create a catalog for each table

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

        selected_models = [node.unique_id for node in nodes + sources]
        return selected_models, parse_manifest(manifest), parse_catalog(catalog)
    except Exception as e:
        raise Exception("Unable to generate partial manifest and catalog") from e


def map_url_to_instance(url, instance):
    # Base URLs and their corresponding patterns
    url_mapping = {
        "https://api.tryaltimate.com": f"https://{instance}.demo.tryaltimate.com",
        "https://api.myaltimate.com": f"https://{instance}.app.myaltimate.com",
        "https://api.getaltimate.com": f"https://{instance}.app.getaltimate.com",
        "http://localhost:8000": f"http://{instance}.localhost:3000",
    }

    # Check if the URL is in the dictionary and return the corresponding instance URL
    return url_mapping.get(url)
