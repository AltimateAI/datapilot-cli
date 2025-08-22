===============================
Pre-commit Hook
===============================

Overview
--------
You can use the pre-commit hook to run DataPilot checks on your dbt project. Do this by following the instructions below:

1. Install pre-commit (https://pre-commit.com/#install)
2. Create a file in the root of the project ``.pre-commit-config.yaml``
3. Add to that file this below:
4. Run ``pre-commit run --all-files`` to test it out

.. code-block:: yaml

    repos:
      - repo: https://github.com/AltimateAI/datapilot-cli
        rev: v0.0.27
        hooks:
          - id: datapilot_run_dbt_checks
            args: [--config-name, "[The Config Name in the SaaS UI]", --token, "[YOUR API KEY]", --instance-name, "[Your tenant name]"]


Performance
-----------

The primary objective is to ensure the pre-commit hook operates swiftly and efficiently, preventing any delay in the development workflow. To achieve this, various optimizations have been applied, focusing on minimizing the time and resources required during execution.

Optimizations
-------------
1. **Partial Catalog Fetching**:
   Instead of retrieving the entire catalog schema, the pre-commit hook is optimized to fetch only the schema of the files being committed. This approach significantly reduces the fetching time and the amount of data processed.

2. **Cost-effective Commands**:
   The hook utilizes commands that avoid activating the warehouses in Snowflake, enhancing cost effectiveness. Specifically, it avoids the use of `dbt docs generate`, which retrieves columns from the information schema and requires warehouse activation, thereby incurring higher costs.

Timing Results for the_tuva_project
-----------------------------------
The following timing results illustrate the efficiency of the pre-commit hook across different scenarios, with varying numbers of files changed in the commit:

- **1 file changed**:
  - DataPilot: 15 seconds
  - Checkpoint: 60 seconds

- **5 files changed**:
  - DataPilot: 16 seconds
  - Checkpoint: 54 seconds

- **10 files changed**:
  - DataPilot: 19 seconds
  - Checkpoint: 54 seconds

- **15 files changed**:
  - DataPilot: 24 seconds
  - Checkpoint: 45 seconds

- **20 files changed**:
  - DataPilot: 19 seconds
  - Checkpoint: 56 seconds

- **Check on all files (309 files)**:
  - DataPilot: 42 seconds
  - Checkpoint: 71 seconds

Conclusion
----------
The optimized pre-commit hook demonstrates a consistent performance improvement, effectively balancing the speed of development against the necessity of maintaining code quality and cost efficiency.
