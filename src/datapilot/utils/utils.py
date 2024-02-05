import json
import os
from pathlib import Path
from typing import Dict

from datapilot.clients.altimate.client import APIClient


def load_json(file_path: str) -> Dict:
    try:
        with Path(file_path).open() as f:
            return json.load(f)
    except FileNotFoundError:
        raise
    except json.decoder.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON file: {file_path}") from e


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


# Will need to change this
base_url = "http://localhost:5001"


def upload_content_to_signed_url(file_path, signed_url):
    api_client = APIClient()

    with Path(file_path).open("rb") as file:
        file_content = file.read()

    return api_client.put(signed_url, data=file_content)


def onboard_manifest(api_token, tenant, dbt_core_integration_id, manifest_path):
    api_client = APIClient(api_token, base_url, tenant)

    endpoint = "/dbt/v1/signed_url"
    params = {"dbt_core_integration_id": dbt_core_integration_id, "file_type": "manifest"}
    signed_url_data = api_client.get(endpoint, params=params)

    if signed_url_data:
        signed_url = signed_url_data.get("url")
        file_id = signed_url_data.get("dbt_core_integration_file_id")
        api_client.log(f"Received signed URL: {signed_url}")
        api_client.log(f"Received File ID: {file_id}")

        upload_response = upload_content_to_signed_url(manifest_path, signed_url)

        if upload_response:
            verify_params = {"dbt_core_integration_file_id": file_id}
            api_client.verify_upload(verify_params)

        else:
            api_client.log(f"Error uploading file: {upload_response.status_code}, {upload_response.text}")

    else:
        api_client.log("Error getting signed URL.")
