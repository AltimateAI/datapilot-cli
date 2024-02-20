import json
import os
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
