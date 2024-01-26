import json
import os
from typing import Dict, Text

from datapilot.clients.altimate.client import APIClient


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


# Will need to change this
base_url = "http://localhost:5001"


def onboard_manifest(api_token, tenant, dbt_core_integration_id, manifest_path):
    api_client = APIClient(api_token, base_url, tenant)

    endpoint = "/dbt/v1/signed_url"
    params = {"dbt_core_integration_id": dbt_core_integration_id, "file_type": "manifest"}
    signed_url_data = api_client.get(endpoint, params=params)

    if signed_url_data:
        signed_url = signed_url_data.get("url")
        file_id = signed_url_data.get("dbt_core_integration_file_id")
        print(f"Received signed URL: {signed_url}")

        with open(manifest_path, "rb") as file:
            file_content = file.read()

        upload_response = api_client.post(signed_url, data=file_content)

        if upload_response:
            endpoint = "/verify_upload"
            verify_params = {"dbt_core_integration_file_id": file_id}
            verify_response = api_client.post(endpoint, params=verify_params)

            if verify_response:
                print("File successfully uploaded and verified.")
                return
            else:
                print(f"Error verifying upload: {verify_response.status_code}, {verify_response.text}")

        else:
            print(f"Error uploading file: {upload_response.status_code}, {upload_response.text}")

    else:
        print("Error getting signed URL.")
