import logging

import requests
from requests.exceptions import ConnectionError
from requests.exceptions import HTTPError
from requests.exceptions import RequestException
from requests.exceptions import Timeout


class APIClient:
    def __init__(self, api_token="", base_url="", tenant=""):
        self.api_token = api_token
        self.base_url = base_url
        self.tenant = tenant
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_headers(self):
        headers = {
            "Content-Type": "application/json",
        }

        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        if self.tenant:
            headers["x-tenant"] = self.tenant

        return headers

    def log(self, message):
        self.logger.debug(message)

    def get(self, endpoint, params=None, timeout=None):
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

        try:
            self.logger.debug(f"Sending GET request for tenant {self.tenant} at url: {url}")
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            # Check if the response was successful
            response.raise_for_status()
            self.logger.debug(f"Received GET response with status: {response.status_code}")
            return response.json()

        except HTTPError as http_err:
            self.logger.debug(f"HTTP Error: {http_err.response.json()} - Status code: {response.status_code}")
        except ConnectionError as conn_err:
            self.logger.error(f"Connection error occurred: {conn_err}")
        except Timeout as timeout_err:
            self.logger.error(f"Timeout error occurred: {timeout_err}")
        except RequestException as req_err:
            self.logger.error(f"Unexpected error occurred: {req_err}")
        except Exception as err:
            self.logger.error(f"An error occurred: {err}")

    def post(self, endpoint, data=None, timeout=None):
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

        self.logger.debug(f"Sending POST request for tenant {self.tenant} at url: {url}")
        response = requests.post(url, headers=headers, json=data, timeout=timeout)
        self.logger.debug(f"Received POST response with status: {response.status_code }")

        return response.json()

    def put(self, endpoint, data, timeout=None):
        url = f"{self.base_url}{endpoint}"

        self.logger.debug(f"Sending PUT request for tenant {self.tenant} at url: {url}")
        response = requests.put(url, data=data, timeout=timeout)
        self.logger.debug(f"Received PUT response with status: {response.status_code}")
        return response

    def verify_upload(self, params=None):
        endpoint = "/dbt/v1/verify_upload"
        self.post(endpoint, data=params)

    def get_signed_url(self, params=None):
        endpoint = "/dbt/v1/signed_url"
        return self.get(endpoint, params=params)

    def validate_credentials(self):
        endpoint = "/dbt/v3/validate-credentials"
        return self.get(endpoint)

    def validate_upload_to_integration(self):
        endpoint = "/dbt/v1/validate-permissions"
        return self.get(endpoint)

    def start_dbt_ingestion(self, params=None):
        endpoint = "/dbt/v1/start_dbt_ingestion"
        return self.post(endpoint, data=params)

    def get_project_governance_llm_checks(self, params=None):
        endpoint = "/project_governance/checks"
        return self.get(endpoint, params=params)

    def run_project_governance_llm_checks(self, manifest, catalog, check_names):
        endpoint = "/project_governance/check/run"
        data = {
            "manifest": manifest,
            "catalog": catalog,
            "check_names": check_names,
        }
        return self.post(endpoint, data=data)
