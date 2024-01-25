import json
import os
from typing import Dict, Text


def load_json(file_path: Text) -> Dict:
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        raise
    except json.decoder.JSONDecodeError:
        raise ValueError(f"Invalid JSON file: {file_path}")


def extract_dir_name_from_file_path(path: Text) -> Text:
    # Handle both Windows and Linux paths using os.path
    # Get root directory name
    return os.path.basename(os.path.dirname(path))


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
    return os.path.dirname(path)
