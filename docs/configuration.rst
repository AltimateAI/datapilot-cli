Advanced Usage
==============

Project Health Configuration
----------------------------

You can configure the project health settings by providing a configuration file. The configuration file is a YAML file that contains the following fields:

.. code-block:: yaml

    version: v1

    # Insights to disable
    disabled_insights:
      - source_staging_model_integrity
      - downstream_source_dependence
      - Duplicate_Sources
      - hard_coded_references
      - rejoining_upstream_concepts
      - model_fanout
      - multiple_sources_joined

    # Define patterns to identify different types of models
    model_type_patterns:
      staging: "^stg_.*"       # Regex for staging models
      mart: "^(mrt_|mart_|fct_|dim_).*"  # Regex for mart models
      intermediate: "^int_.*"  # Regex for intermediate models
      base: "^base_.*"         # Regex for base models

    # Configure insights
    insights:
      # Set minimum test coverage percent and severity for 'Low Test Coverage in DBT Models'
      dbt_low_test_coverage:
        min_test_coverage_percent: 30
        severity: WARNING

      # Configure maximum fanout for 'Model Fanout Analysis'
      model_fanout.max_fanout: 10

      # Configure maximum fanout for 'Source Fanout Analysis'
      source_fanout.max_fanout: 10

      # Define model types considered as downstream for 'Staging Models Dependency Check'
      staging_models_dependency.downstream_model_types:
        - mart

Key Sections of the config file
-------------------------------

- disabled_insights: Insights that you want to disable
- model_type_patterns: Regex patterns to identify different model types like staging, mart, etc.
- insights: Custom configurations for each insight. For each insight, you can set specific thresholds, severity levels, or other parameters.

Severity can have 3 values -> INFO, WARNING, ERROR

Overriding default configs for the insights
-------------------------------------------

To change the severity level or set a threshold, modify the corresponding insight under the insights section. For example:

.. code-block:: yaml

    insights:
      dbt_low_test_coverage:
        severity: WARNING

For insights with more complex configurations (like fanout thresholds or model types), you need to specify the insight name and corresponding parameter under insights. For example:

.. code-block:: yaml

    insights:
      model_fanout.max_fanout: 10
