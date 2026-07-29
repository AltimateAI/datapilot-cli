import logging

import pytest
from click.testing import CliRunner

from datapilot.cli.main import datapilot
from datapilot.utils import logging_utils
from datapilot.utils.logging_utils import DEBUG_ENV_VAR


@pytest.fixture(autouse=True)
def _reset_logging_state(monkeypatch):
    monkeypatch.setattr(logging_utils, "_debug_enabled", False)
    monkeypatch.delenv(DEBUG_ENV_VAR, raising=False)
    original_level = logging.getLogger().level
    yield
    logging.getLogger().setLevel(original_level)


def invoke_project_health(env=None, extra_args=()):
    """project-health runs fully offline, so it exercises the flag without network access."""
    return CliRunner().invoke(
        datapilot,
        ["dbt", "project-health", "--manifest-path", "tests/data/manifest_v11.json", *extra_args],
        env=env or {},
    )


class TestDebugFlag:
    def test_flag_is_advertised_in_help(self):
        result = CliRunner().invoke(datapilot, ["dbt", "onboard", "--help"])

        assert result.exit_code == 0
        assert "--debug" in result.output
        assert DEBUG_ENV_VAR in result.output

    def test_flag_enables_debug_logging(self):
        result = invoke_project_health(extra_args=["--debug"])

        assert result.exit_code == 0
        assert logging_utils.is_debug_enabled() is True
        assert logging.getLogger().level == logging.DEBUG

    def test_without_flag_stays_at_info(self):
        result = invoke_project_health()

        assert result.exit_code == 0
        assert logging_utils.is_debug_enabled() is False
        assert logging.getLogger().level == logging.INFO


class TestDebugEnvVar:
    def test_env_var_enables_debug_logging(self):
        result = invoke_project_health(env={DEBUG_ENV_VAR: "1"})

        assert result.exit_code == 0
        assert logging_utils.is_debug_enabled() is True
        assert logging.getLogger().level == logging.DEBUG

    def test_env_var_off_stays_at_info(self):
        result = invoke_project_health(env={DEBUG_ENV_VAR: "0"})

        assert result.exit_code == 0
        assert logging_utils.is_debug_enabled() is False
        assert logging.getLogger().level == logging.INFO
