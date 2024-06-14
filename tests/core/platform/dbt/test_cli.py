# test_app.py
from click.testing import CliRunner

from datapilot.core.platforms.dbt.cli.cli import project_health


def test_project_health_with_required_and_optional_args():
    runner = CliRunner()
    manifest_path = "tests/data/manifest_v11.json"
    catalog_path = "tests/data/catalog_v1.json"
    config_path = "tests/data/config.yml"

    # Simulate command invocation
    result = runner.invoke(project_health, ["--manifest-path", manifest_path, "--catalog-path", catalog_path, "--config-path", config_path])

    assert result.exit_code == 0  # Ensure the command executed successfully
    # Add more assertions here to validate the behavior of your command,
    # for example, checking that the output contains expected text.
    assert "-----------" in result.output


def test_project_health_with_only_required_arg():
    runner = CliRunner()
    manifest_path = "tests/data/manifest_v11.json"

    # Simulate command invocation without optional arguments
    result = runner.invoke(
        project_health,
        [
            "--manifest-path",
            manifest_path,
        ],
    )

    assert result.exit_code == 0  # Ensure the command executed successfully
    # Validate behavior for when only the required argument is provided
    assert "-----------" in result.output


def test_project_health_with_only_required_arg_version1_6():
    runner = CliRunner()
    manifest_path = "tests/data/manifest_v10.json"

    # Simulate command invocation without optional arguments
    result = runner.invoke(
        project_health,
        [
            "--manifest-path",
            manifest_path,
        ],
    )

    assert result.exit_code == 0  # Ensure the command executed successfully
    # Validate behavior for when only the required argument is provided
    assert "-----------" in result.output


def test_project_health_with_macro_args():
    runner = CliRunner()
    manifest_path = "tests/data/manifest_v10macroargs.json"

    # Simulate command invocation without optional arguments
    result = runner.invoke(
        project_health,
        [
            "--manifest-path",
            manifest_path,
        ],
    )

    assert result.exit_code == 0  # Ensure the command executed successfully
    # Validate behavior for when only the required argument is provided
    assert "-----------" in result.output

    manifest_path = "tests/data/manifest_v11macroargs.json"

    # Simulate command invocation without optional arguments
    result = runner.invoke(
        project_health,
        [
            "--manifest-path",
            manifest_path,
        ],
    )

    assert result.exit_code == 0  # Ensure the command executed successfully
    # Validate behavior for when only the required argument is provided
    assert "-----------" in result.output


def test_project_health_with_required_and_optional_args_v12():
    runner = CliRunner()
    manifest_path = "tests/data/manifest_v12.json"
    catalog_path = "tests/data/catalog_v12.json"
    config_path = "tests/data/config.yml"

    # Simulate command invocation
    result = runner.invoke(project_health, ["--manifest-path", manifest_path, "--catalog-path", catalog_path, "--config-path", config_path])

    assert result.exit_code == 0  # Ensure the command executed successfully
    # Add more assertions here to validate the behavior of your command,
    # for example, checking that the output contains expected text.
    assert "-----------" in result.output
