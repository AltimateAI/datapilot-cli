import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Dict


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


def generate_partial_manifest_catalog(changed_files, manifest_path: str, catalog_path: str):
    models = [Path(f).stem for f in changed_files]

    for model in models:
        subprocess.run(["dbt", "compile", "--models", model])  # noqa

    manifest_file = Path("target/manifest.json")
    catalog_file = Path("target/catalog.json")

    with manifest_file.open() as f:
        manifest = json.load(f)

    with catalog_file.open() as f:
        catalog = json.load(f)

    manifest["nodes"] = {k: v for k, v in manifest["nodes"].items() if v["name"] in models}
    catalog["nodes"] = {k: v for k, v in catalog["nodes"].items() if v["metadata"]["name"] in models}

    with Path.open(manifest_path, "w") as f:
        json.dump(manifest, f)
    with Path.open(catalog_path, "w") as f:
        json.dump(catalog, f)
