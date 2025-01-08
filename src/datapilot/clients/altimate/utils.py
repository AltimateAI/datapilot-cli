import os
from pathlib import Path
from typing import Dict
from typing import Optional

import click
from requests import Response

from datapilot.clients.altimate.client import APIClient


def check_token_and_instance(
    token: Optional[str],
    instance_name: Optional[str],
):
    if not token:
        token = os.environ.get("ALTIMATE_API_KEY")

    if not instance_name:
        instance_name = os.environ.get("ALTIMATE_INSTANCE_NAME")

    if not token or not instance_name:
        click.echo(
            "Error: API TOKEN and instance name is required. Please provide a valid API token."
            " You can pass it as command line arguments or set it using environment variables like "
            "ALTIMATE_API_KEY and ALTIMATE_INSTANCE_NAME."
        )
        return


def upload_content_to_signed_url(file_path, signed_url) -> Response:
    api_client = APIClient()

    with Path(file_path).open("rb") as file:
        file_content = file.read()

    return api_client.put(signed_url, data=file_content)


def validate_credentials(
    token,
    backend_url,
    tenant,
) -> Response:
    api_client = APIClient(api_token=token, base_url=backend_url, tenant=tenant)
    return api_client.validate_credentials()


def validate_permissions(
    token,
    backend_url,
    tenant,
) -> Response:
    api_client = APIClient(api_token=token, base_url=backend_url, tenant=tenant)
    return api_client.validate_upload_to_integration()


def onboard_file(api_token, tenant, dbt_core_integration_id, dbt_core_integration_environment, file_type, file_path, backend_url) -> Dict:
    api_client = APIClient(api_token, base_url=backend_url, tenant=tenant)

    params = {
        "dbt_core_integration_id": dbt_core_integration_id,
        "dbt_core_integration_environment_type": dbt_core_integration_environment,
        "file_type": file_type,
    }
    signed_url_data = api_client.get_signed_url(params)
    if signed_url_data:
        signed_url = signed_url_data.get("url")
        file_id = signed_url_data.get("dbt_core_integration_file_id")
        api_client.log(f"Received signed URL: {signed_url}")
        api_client.log(f"Received File ID: {file_id}")

        upload_response = upload_content_to_signed_url(file_path, signed_url)

        if upload_response:
            verify_params = {"dbt_core_integration_file_id": file_id}
            api_client.verify_upload(verify_params)
            return {"ok": True}
        else:
            api_client.log(f"Error uploading file: {upload_response.status_code}, {upload_response.text}")
            return {"ok": False, "message": f"Error uploading file: {upload_response.status_code}, {upload_response.text}"}

    else:
        api_client.log("Error getting signed URL.")
        return {
            "ok": False,
            "message": "Error in uploading the manifest.                                                                                                                              ",
        }


def start_dbt_ingestion(api_token, tenant, dbt_core_integration_id, dbt_core_integration_environment, backend_url):
    api_client = APIClient(api_token, base_url=backend_url, tenant=tenant)
    params = {
        "dbt_core_integration_id": dbt_core_integration_id,
        "dbt_core_integration_environment_type": dbt_core_integration_environment,
    }
    data = api_client.start_dbt_ingestion(params)
    if data and data.get("ok"):
        return {"ok": True}
    else:
        api_client.log("Error starting dbt ingestion worker")
        return {
            "ok": False,
            "message": "Error starting dbt ingestion worker.                                                                                                                              ",
        }


def get_project_governance_llm_checks(
    api_token,
    tenant,
    backend_url,
):
    api_client = APIClient(api_token=api_token, base_url=backend_url, tenant=tenant)
    return api_client.get_project_governance_llm_checks()


def run_project_governance_llm_checks(
    api_token,
    tenant,
    backend_url,
    manifest,
    catalog,
    check_names,
):
    api_client = APIClient(api_token=api_token, base_url=backend_url, tenant=tenant)
    return api_client.run_project_governance_llm_checks(manifest, catalog, check_names)
