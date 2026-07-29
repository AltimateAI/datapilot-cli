import logging

import pytest

from datapilot.utils import logging_utils
from datapilot.utils.logging_utils import DEBUG_ENV_VAR
from datapilot.utils.logging_utils import configure_logging
from datapilot.utils.logging_utils import debug_enabled_via_env
from datapilot.utils.logging_utils import is_debug_enabled
from datapilot.utils.logging_utils import redact_url


@pytest.fixture(autouse=True)
def _reset_logging_state(monkeypatch):
    """Debug mode is sticky by design, so reset it between tests."""
    monkeypatch.setattr(logging_utils, "_debug_enabled", False)
    monkeypatch.delenv(DEBUG_ENV_VAR, raising=False)
    original_level = logging.getLogger().level
    yield
    logging.getLogger().setLevel(original_level)


class TestDebugEnabledViaEnv:
    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", " True "])
    def test_truthy_values(self, monkeypatch, value):
        monkeypatch.setenv(DEBUG_ENV_VAR, value)

        assert debug_enabled_via_env() is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", "", "   "])
    def test_falsy_values(self, monkeypatch, value):
        monkeypatch.setenv(DEBUG_ENV_VAR, value)

        assert debug_enabled_via_env() is False

    def test_unset(self):
        assert debug_enabled_via_env() is False


class TestConfigureLogging:
    def test_default_is_info(self):
        assert configure_logging() is False
        assert is_debug_enabled() is False
        assert logging.getLogger().level == logging.INFO

    def test_debug_argument_sets_debug_level(self):
        assert configure_logging(debug=True) is True
        assert logging.getLogger().level == logging.DEBUG

    def test_env_var_sets_debug_level(self, monkeypatch):
        monkeypatch.setenv(DEBUG_ENV_VAR, "1")

        assert configure_logging() is True
        assert logging.getLogger().level == logging.DEBUG

    def test_env_var_off_stays_at_info(self, monkeypatch):
        monkeypatch.setenv(DEBUG_ENV_VAR, "0")

        assert configure_logging() is False
        assert logging.getLogger().level == logging.INFO

    def test_debug_is_sticky_across_calls(self):
        """An import-time configure_logging() must not undo a --debug flag, or vice versa."""
        configure_logging(debug=True)
        configure_logging(debug=False)

        assert is_debug_enabled() is True
        assert logging.getLogger().level == logging.DEBUG

    def test_debug_records_reach_a_handler(self, caplog):
        configure_logging(debug=True)

        with caplog.at_level(logging.DEBUG):
            logging.getLogger("APIClient").debug("HTTP Error: boom")

        assert "HTTP Error: boom" in caplog.text


class TestRedactUrl:
    def test_url_without_query_is_unchanged(self):
        url = "https://api.myaltimate.com/dbt/v1/signed_url"

        assert redact_url(url) == url

    def test_presigned_url_credentials_are_removed(self):
        url = (
            "https://altimate-datapilot-freemium-prod.s3.amazonaws.com/prd/tenant%3Delastic/manifest.json"
            "?AWSAccessKeyId=AKIAVXVSOK5H3JQFSSU4&Signature=2LV9nU5JHPVOFz&Expires=1785271367"
        )

        redacted = redact_url(url)

        assert "AKIAVXVSOK5H3JQFSSU4" not in redacted
        assert "2LV9nU5JHPVOFz" not in redacted
        assert redacted == ("https://altimate-datapilot-freemium-prod.s3.amazonaws.com/prd/tenant%3Delastic/manifest.json?<redacted>")

    def test_empty_values(self):
        assert redact_url("") == ""
        assert redact_url(None) == ""
