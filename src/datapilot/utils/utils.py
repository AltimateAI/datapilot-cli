import json
import os
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


class DBTNode(BaseModel):
    unique_id: str
    name: str
    resource_type: str
    table: str = ""


def generate_partial_manifest_catalog(changed_files, manifest_path: str, catalog_path: str):
    models = [Path(f).stem for f in changed_files]

    subprocess.run(["dbt", "compile", "--models", " ".join(models)])  # noqa

    manifest_file = Path("/Users/gaurp/jaffle_shop/target/manifest.json")
    with manifest_file.open() as f:
        manifest = json.load(f)

    nodes = []
    for node in manifest["nodes"]:
        nodes.append(DBTNode(**manifest["nodes"][node]))

    for node in manifest["sources"]:
        nodes.append(DBTNode(**manifest["sources"][node]))

    nodes_str = ",\n".join(
        [
            "{" + f'"name":"{node.name}","resource_type":"{node.resource_type}","unique_id":"{node.unique_id}","table":""' + "}"
            for node in nodes
        ]
    )

    query = (
        "{% set result = {} %}{% set nodes = ["
        + nodes_str
        + '] %}{% for n in nodes %}{% if n["resource_type"] == "source" %}{% set columns = adapter.get_columns_in_relation(source(n["name"], n["table"])) %}{% else %}{% set columns = adapter.get_columns_in_relation(ref(n["name"])) %}{% endif %}{% set new_columns = [] %}{% for column in columns %}{% do new_columns.append({"column": column.name, "dtype": column.dtype}) %}{% endfor %}{% do result.update({n["unique_id"]:new_columns}) %}{% endfor %}{{ tojson(result) }}'
    )

    subprocess.run(["dbt", "compile", "--inline", query])  # noqa

    # Will need to get catalog by getting the json output from above query

    catalog_file = Path("target/catalog.json")
    with catalog_file.open() as f:
        catalog = json.load(f)

    with Path.open(manifest_path, "w") as f:
        json.dump(manifest, f)
    with Path.open(catalog_path, "w") as f:
        json.dump(catalog, f)


if __name__ == "__main__":
    print("Running main")
    print(generate_partial_manifest_catalog([], "/Users/gaurp/Desktop/manifest.json", ""))
    print("Done running main")
