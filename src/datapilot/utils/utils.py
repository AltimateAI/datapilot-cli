import json
import os
import re
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Dict

from pydantic import BaseModel


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


class ModelNode(BaseModel):
    unique_id: str
    name: str
    resource_type: str


class SourceNode(BaseModel):
    unique_id: str
    name: str
    resource_type: str
    table: str = ""
    alias: str = ""


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
        return "CHAR"
    elif re.match(r".*varchar.*", dtype):
        return "VARCHAR"
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


def generate_partial_manifest_catalog(changed_files, manifest_path: str, catalog_path: str):
    # changed_files = [
    #     "/Users/gaurp/the_tuva_project_dbt_cloud/models/cms_hcc/final/cms_hcc__patient_risk_factors.sql",
    # ]
    models = [Path(f).stem for f in changed_files]

    subprocess.run(["dbt", "parse"], cwd="/Users/gaurp/the_tuva_project_dbt_cloud")  # noqa

    manifest_file = Path("/Users/gaurp/the_tuva_project_dbt_cloud/target/manifest.json")
    with manifest_file.open() as f:
        manifest = json.load(f)

    nodes = []
    sources = []
    for node in manifest["nodes"].values():
        if node["name"] in models:
            if node["resource_type"] == "model":
                nodes.append(ModelNode(**node))

    for node in manifest["sources"].values():
        sources.append(
            SourceNode(unique_id=node["unique_id"], name=node["source_name"], resource_type=node["resource_type"], table=node["identifier"])
        )

    nodes_str = []
    for node in nodes:
        nodes_str.append(
            {
                "name": node.name,
                "resource_type": node.resource_type,
                "unique_id": node.unique_id,
            }
        )

    for source in sources:
        nodes_str.append(
            {
                "name": source.name,
                "resource_type": source.resource_type,
                "unique_id": source.unique_id,
                "table": source.table,
            }
        )

        # query = (
        #     """'{% set result = {} %}{% set nodes ="""
        #     + str(nodes_str)
        #     + """%}{% for n in nodes %}  {% if n["resource_type"] == "source" %}    {% set columns = adapter.get_columns_in_relation(source(n["name"], n["table"])) %}  {% else %}    {% set columns = adapter.get_columns_in_relation(ref(n["name"])) %}  {% endif %}  {% set new_columns = [] %}  {% for column in columns %}    {% do new_columns.append({"column": column.name, "dtype": column.dtype}) %}  {% endfor %}  {% do result.update({n["unique_id"]:new_columns}) %}{% endfor %}{{ tojson(result) }}' """
        # )

        query = """
  {%- set sql -%}
    describe table analytics.public.customers
  {%- endset -%}
  {%- set result = run_query(sql) -%}

  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many columns in relation analytics.public.customers! dbt can only get
      information about relations with fewer than {{ maximum }} columns.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}

  {% set columns = [] %}
  {% for row in result %}
    {% do columns.append(api.Column.from_description(row['name'], row['type'])) %}
  {% endfor %}
  {% do return(columns) %}
        """

    query = query.strip()
    print(query)

    dbt_compile = subprocess.run(
        ["dbt", "compile", "--inline", query],  # noqa
        cwd="/Users/gaurp/the_tuva_project_dbt_cloud",
        capture_output=True,
        text=True,
    )

    dbt_compile_output = dbt_compile.stdout

    print(dbt_compile_output)

    compiled_inline_node = dbt_compile_output.split("Compiled inline node is:")[1].strip().replace("'", "").strip()

    compiled_dict = json.loads(compiled_inline_node)

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

    # we need to get all columns  from compiled_dict which is supposed to be a dict
    # with list of columns for each model and source and each column will be a dict with column name and dtype
    # we need to convert this to a list of columns for each model and source and each column will be a dict with format like
    # we are getting dtype like VARCHAR, BOOLEAN, NUMBER etc. from the compiled_dict
    # "columns": {"customer_id": {"type": "integer", "index": 1, "name": "customer_id","comment": null} i.e. we need to add index and name to the dict
    # index will be autoincrementing from 1 to n for each column in the list and comment will be null. we need to convert the type to text, integer, bigint, date, etc

    for node in nodes:
        columns = []
        for column in compiled_dict[node.unique_id]:
            column_type = get_column_type(column["dtype"])
            columns.append(
                {
                    "type": column_type,
                    "index": len(columns) + 1,
                    "name": column["column"],
                    "comment": None,
                }
            )

        catalog["nodes"] = {
            node.unique_id: {
                "metadata": {
                    "type": "BASE TABLE",
                    "schema": manifest["nodes"][node.unique_id]["schema"],
                    "name": node.name,
                    "database": manifest["nodes"][node.unique_id]["database"],
                    "comment": None,
                    "owner": None,
                },
                "columns": columns,
                "stats": {},
                "unique_id": node.unique_id,
            }
        }

    for source in sources:
        columns = []
        for column in compiled_dict[source.unique_id]:
            column_type = get_column_type(column["dtype"])
            columns.append(
                {
                    "type": column_type,
                    "index": len(columns) + 1,
                    "name": column["column"],
                    "comment": None,
                }
            )

        catalog["sources"] = {
            source.unique_id: {
                "metadata": {
                    "type": "BASE TABLE",
                    "schema": manifest["sources"][source.unique_id]["schema"],
                    "name": source.name,
                    "database": manifest["sources"][source.unique_id]["database"],
                    "comment": None,
                    "owner": None,
                },
                "columns": columns,
                "stats": {},
                "unique_id": source.unique_id,
            }
        }

    with Path.open(manifest_path, "w") as f:
        json.dump(manifest, f)
    with Path.open(catalog_path, "w") as f:
        json.dump(catalog, f)


if __name__ == "__main__":
    print("Running main")
    print(generate_partial_manifest_catalog([], "/Users/gaurp/Desktop/manifest.json", ""))
    print("Done running main")
