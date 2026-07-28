import logging
from unittest.mock import patch

import pytest
from requests import Response

from datapilot.clients.altimate.client import APIClient
from datapilot.utils import logging_utils
from datapilot.utils.logging_utils import DEBUG_ENV_VAR
from datapilot.utils.logging_utils import configure_logging

PRESIGNED_URL = (
    "https://altimate-datapilot-freemium-prod.s3.amazonaws.com/prd/tenant%3Delastic/manifest.json"
    "?AWSAccessKeyId=AKIAVXVSOK5H3JQFSSU4&Signature=2LV9nU5JHPVOFz&Expires=1785271367"
)


@pytest.fixture(autouse=True)
def _reset_logging_state(monkeypatch):
    monkeypatch.setattr(logging_utils, "_debug_enabled", False)
    monkeypatch.delenv(DEBUG_ENV_VAR, raising=False)
    original_level = logging.getLogger().level
    yield
    logging.getLogger().setLevel(original_level)


def make_response(status_code=200):
    response = Response()
    response.status_code = status_code
    response._content = b""
    return response


class TestPresignedUrlRedaction:
    def test_put_does_not_log_aws_credentials(self, caplog):
        client = APIClient()

        with caplog.at_level(logging.DEBUG), patch("requests.put", return_value=make_response()):
            client.put(PRESIGNED_URL, data=b"{}")

        assert "AKIAVXVSOK5H3JQFSSU4" not in caplog.text
        assert "2LV9nU5JHPVOFz" not in caplog.text
        # The object path is still logged, which is the part that aids debugging.
        assert "manifest.json" in caplog.text
        assert "<redacted>" in caplog.text

    def test_get_logs_request_params(self, caplog):
        """Params identify the integration id/env, and contain no secrets."""
        client = APIClient(api_token="secret-token", base_url="https://api.myaltimate.com", tenant="elastic")  # noqa: S106
        params = {"dbt_core_integration_id": "2", "dbt_core_integration_environment_type": "PROD"}

        with caplog.at_level(logging.DEBUG), patch("requests.get", return_value=make_response()):
            client.get("/dbt/v1/signed_url", params=params)

        assert "dbt_core_integration_environment_type" in caplog.text
        assert "PROD" in caplog.text
        assert "secret-token" not in caplog.text


class TestUrllib3Silencing:
    def test_urllib3_is_held_at_info_in_debug_mode(self):
        """urllib3 logs whole request lines, presigned query string included."""
        configure_logging(debug=True)

        assert logging.getLogger().level == logging.DEBUG
        assert logging.getLogger("urllib3").level == logging.INFO
