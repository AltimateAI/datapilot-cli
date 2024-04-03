====================
Using DataPilot Hooks
====================

DataPilot provides a pre-commit hook that can be integrated into your workflow to enhance the development process of your dbt projects. This guide will walk you through the setup and usage of the DataPilot pre-commit hook.

Installation
------------

To use the DataPilot pre-commit hook, follow these steps:

1. Install the `pre-commit` package if you haven't already:

```
pip install pre-commit
```

2. Add the following configuration to your .pre-commit-config.yaml file in the root of your repository:

```
    repos:
  - repo: https://github.com/AltimateAI/datapilot-cli
    rev: <revision>
    hooks:
      - id: datapilot_run_dbt_checks
        args: ["--config-path", "path/to/your/config/file"]
```

Replace <revision> with the desired revision of the DataPilot repository and "path/to/your/config/file" with the path to your configuration file.

3. Install the pre-commit hook:

```
pre-commit install
```

Usage
-----

Once the hook is installed, it will run automatically before each commit. If any issues are detected, the commit will be aborted, and you will be prompted to fix the issues before retrying the commit.


If you want to manually run all pre-commit hooks on a repository, run `pre-commit run --all-files`. To run individual hooks use `pre-commit run <hook_id>`.


Feedback and Contributions
--------------------------

If you encounter any issues or have suggestions for improvements, please feel free to open an issue or pull request on the DataPilot GitHub repository at https://github.com/AltimateAI/datapilot-cli.

Thank you for using DataPilot!
