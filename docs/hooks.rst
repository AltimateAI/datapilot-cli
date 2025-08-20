====================
Using DataPilot Hooks
====================

DataPilot provides a pre-commit hook that can be integrated into your workflow to enhance the development process of your dbt projects. This guide will walk you through the setup and usage of the DataPilot pre-commit hook.

Installation
------------

To use the DataPilot pre-commit hook, follow these steps:

1. Install the `pre-commit` package if you haven't already:

.. code-block:: shell

    pip install pre-commit

2. Add the following configuration to your .pre-commit-config.yaml file in the root of your repository:

.. code-block:: yaml

    repos:
      - repo: https://github.com/AltimateAI/datapilot-cli
        rev: v0.0.23  # Use a specific version tag, not 'main'
        hooks:
          - id: datapilot_run_dbt_checks
            args: [
              "--config-path", "./datapilot-config.yaml",
              "--token", "${DATAPILOT_TOKEN}",
              "--instance-name", "${DATAPILOT_INSTANCE}"
            ]

Configuration Options
---------------------

The DataPilot pre-commit hook supports several configuration options:

**Required Configuration:**

- ``rev``: Always use a specific version tag (e.g., ``v0.0.23``) instead of ``main`` for production stability

**Optional Arguments:**

- ``--config-path``: Path to your DataPilot configuration file
- ``--token``: Your API token for authentication (can use environment variables)
- ``--instance-name``: Your tenant/instance name (can use environment variables)
- ``--backend-url``: Backend URL (defaults to https://api.myaltimate.com)
- ``--config-name``: Name of config to use from API
- ``--base-path``: Base path of the dbt project (defaults to current directory)

**Environment Variables:**

You can use environment variables for sensitive information:

.. code-block:: yaml

    repos:
      - repo: https://github.com/AltimateAI/datapilot-cli
        rev: v0.0.23
        hooks:
          - id: datapilot_run_dbt_checks
            args: [
              "--config-path", "./datapilot-config.yaml",
              "--token", "${DATAPILOT_TOKEN}",
              "--instance-name", "${DATAPILOT_INSTANCE}"
            ]

**Configuration File Example:**

Create a ``datapilot-config.yaml`` file in your project root:

.. code-block:: yaml

    # DataPilot Configuration
    disabled_insights:
      - "hard_coded_references"
      - "duplicate_sources"

    # Custom settings for your project
    project_settings:
      max_fanout: 10
      require_tests: true

3. Install the pre-commit hook:

.. code-block:: shell

    pre-commit install

Usage
-----

Once the hook is installed, it will run automatically before each commit. The hook will:

1. **Validate Configuration**: Check that your config file exists and is valid
2. **Authenticate**: Use your provided token and instance name to authenticate
3. **Analyze Changes**: Only analyze files that have changed in the commit
4. **Report Issues**: Display any issues found and prevent the commit if problems are detected

**Manual Execution:**

To manually run all pre-commit hooks on a repository:

.. code-block:: shell

    pre-commit run --all-files

To run individual hooks:

.. code-block:: shell

    pre-commit run datapilot_run_dbt_checks

**Troubleshooting:**

- **Authentication Issues**: Ensure your token and instance name are correctly set
- **Empty Config Files**: The hook will fail if your config file is empty or invalid
- **No Changes**: If no relevant files have changed, the hook will skip execution
- **Network Issues**: Ensure you have access to the DataPilot API

Best Practices
-------------

1. **Use Version Tags**: Always specify a version tag in the ``rev`` field, never use ``main``
2. **Environment Variables**: Use environment variables for sensitive information like tokens
3. **Configuration Files**: Create a dedicated config file for your project settings
4. **Regular Updates**: Update to new versions when they become available
5. **Team Coordination**: Ensure all team members use the same configuration

Example Complete Setup
---------------------

Here's a complete example of a ``.pre-commit-config.yaml`` file:

.. code-block:: yaml

    # .pre-commit-config.yaml
    exclude: '^(\.tox|ci/templates|\.bumpversion\.cfg)(/|$)'

    repos:
      - repo: https://github.com/astral-sh/ruff-pre-commit
        rev: v0.1.14
        hooks:
          - id: ruff
            args: [--fix, --exit-non-zero-on-fix, --show-fixes]

      - repo: https://github.com/psf/black
        rev: 23.12.1
        hooks:
          - id: black

      - repo: https://github.com/AltimateAI/datapilot-cli
        rev: v0.0.23
        hooks:
          - id: datapilot_run_dbt_checks
            args: [
              "--config-path", "./datapilot-config.yaml",
              "--token", "${DATAPILOT_TOKEN}",
              "--instance-name", "${DATAPILOT_INSTANCE}"
            ]

Feedback and Contributions
--------------------------

If you encounter any issues or have suggestions for improvements, please feel free to open an issue or pull request on the DataPilot GitHub repository at https://github.com/AltimateAI/datapilot-cli.

Thank you for using DataPilot!
