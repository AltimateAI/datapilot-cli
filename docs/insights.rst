Insights
========

The following insights are available in DataPilot:

1. Modelling Insights
---------------------

+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| Name                                 | Description                                      | Files Required  | Overrides                 |
+======================================+==================================================+=================+===========================+
| source_staging_model_integrity       | | Ensures each source has a dedicated            | Manifest        | None                      |
|                                      | | staging model and is not directly              |                 |                           |
|                                      | | joined to downstream models.                   |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| downstream_source_dependence         | | Evaluates if downstream models                 | Manifest        | None                      |
|                                      | | (marts or intermediates) are improperly        |                 |                           |
|                                      | | dependent directly on a source. This           |                 |                           |
|                                      | | check ensures that all downstream              |                 |                           |
|                                      | | models depend on staging models,               |                 |                           |
|                                      | | not directly on the source nodes.              |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| Duplicate_Sources                    | | Identifies cases where multiple source         | Manifest        | None                      |
|                                      | | nodes in a dbt project refer to the            |                 |                           |
|                                      | | same database object. Ensures that each        |                 |                           |
|                                      | | database object is represented by a single,    |                 |                           |
|                                      | | unique source node.                            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| hard_coded_references                | | Identifies instances where SQL code            | Manifest        | None                      |
|                                      | | within models contains hard-coded references,  |                 |                           |
|                                      | | which can obscure data lineage and complicate  |                 |                           |
|                                      | | project maintenance.                           |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| rejoining_upstream_concepts          | | Detects scenarios where a parent’s direct      | Manifest        | None                      |
|                                      | | child is also a direct child of another        |                 |                           |
|                                      | | one of the parent’s direct children, indicating|                 |                           |
|                                      | | potential loops or unnecessary complexity      |                 |                           |
|                                      | | in the DAG.                                    |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| model_fanout                         | | Assesses parent models to identify             | Manifest        | max_fanout                |
|                                      | | high fanout scenarios, which may               |                 |                           |
|                                      | | indicate opportunities for more                |                 |                           |
|                                      | | efficient transformations in the               |                 |                           |
|                                      | | BI layer or better positioning                 |                 |                           |
|                                      | | of common business logic upstream              |                 |                           |
|                                      | | in the data pipeline.                          |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| multiple_sources_joined              | | Checks if a model directly joins               | Manifest        | None                      |
|                                      | | multiple source tables, encouraging            |                 |                           |
|                                      | | the use of a single staging model              |                 |                           |
|                                      | | per source for downstream models               |                 |                           |
|                                      | | to enhance data consistency                    |                 |                           |
|                                      | | and maintainability.                           |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| root_model                           | | Identifies models without direct               | Manifest        | None                      |
|                                      | | parents, either sources or other               |                 |                           |
|                                      | | models within the dbt project.                 |                 |                           |
|                                      | | Ensures all models can be traced               |                 |                           |
|                                      | | back to a source or interconnected             |                 |                           |
|                                      | | within the project, which is crucial           |                 |                           |
|                                      | | for clear data lineage and project             |                 |                           |
|                                      | | integrity.                                     |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| source_fanout                        | | Evaluates sources for high fanout,             | Manifest        | max_fanout                |
|                                      | | identifying when a single source               |                 |                           |
|                                      | | has a large number of direct child             |                 |                           |
|                                      | | models. High fanout may indicate               |                 |                           |
|                                      | | an overly complex or source reliant            |                 |                           |
|                                      | | data model, potentially introducing            |                 |                           |
|                                      | | risks and complicating maintenance             |                 |                           |
|                                      | | and scalability.                               |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| staging_models_dependency            | | Checks whether staging models depend           | Manifest        | None                      |
|                                      | | on downstream models, rather than              |                 |                           |
|                                      | | on source or raw data models. Staging          |                 |                           |
|                                      | | models should ideally depend on                |                 |                           |
|                                      | | upstream data sources to maintain              |                 |                           |
|                                      | | a clear and logical data flow.                 |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| staging_models_on_staging            | | Checks if staging models are dependent         | Manifest        | None                      |
|                                      | | on other staging models instead of             |                 |                           |
|                                      | | on source or raw data models, ensuring         |                 |                           |
|                                      | | that staging models are used                   |                 |                           |
|                                      | | appropriately to maintain a clear              |                 |                           |
|                                      | | and logical data flow from sources             |                 |                           |
|                                      | | to staging.                                    |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| unused_sources                       | | Identifies sources that are defined            | Manifest        | None                      |
|                                      | | in the project’s YML files but not             |                 |                           |
|                                      | | used in any models or sources. They            |                 |                           |
|                                      | | may have become redundant due to               |                 |                           |
|                                      | | model deprecation, contributing to             |                 |                           |
|                                      | | unnecessary complexity and clutter             |                 |                           |
|                                      | | in the dbt project.                            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+

2. Performance Insights
---------------------

+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| Name                                 | Description                                      | Files Required  | Overrides                 |
+======================================+==================================================+=================+===========================+
| chain_view_linking                   | | Analyzes the dbt project to identify           | Manifest        | None                      |
|                                      | | long chains of non materialized                |                 |                           |
|                                      | | models (views and ephemerals).                 |                 |                           |
|                                      | | Such long chains can result in increased       |                 |                           |
|                                      | | runtime for models built on top of them        |                 |                           |
|                                      | | due to extended computation and                |                 |                           |
|                                      | | memory usage.                                  |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| exposure_parent_bad_materialization  | | Evaluates the materialization types of         | Manifest        | None                      |
|                                      | | parent models of exposures to ensure           |                 |                           |
|                                      | | they rely on transformed dbt models            |                 |                           |
|                                      | | or metrics rather than raw sources,            |                 |                           |
|                                      | | and checks if these parent models are          |                 |                           |
|                                      | | materialized efficiently for performance       |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+

3. Governance Insights
---------------------

+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| Name                                 | Description                                      | Files Required  | Overrides                 |
+======================================+==================================================+=================+===========================+
| documentation_on_stale_columns       | | Checks for columns that are documented         | Manifest,       | None                      |
|                                      | | in the dbt project but have been removed       | Catalog         |                           |
|                                      | | from their respective models.                  |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| exposures_dependent_on_private_models| | Detects if exposures in the dbt project        | Manifest        | None                      |
|                                      | | are dependent on private models. Recommends    |                 |                           |
|                                      | | using public, well documented, and             |                 |                           |
|                                      | | contracted models as trusted data              |                 |                           |
|                                      | | sources for downstream consumption.            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| public_models_without_contracts      | | Identifies public models in the dbt project    | Manifest        | None                      |
|                                      | | that are accessible to all downstream          |                 |                           |
|                                      | | consumers but lack contracts specifying        |                 |                           |
|                                      | | data types and columns.                        |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| missing_documentation                | | Detects columns and models that don’t          | Manifest,       | None                      |
|                                      | | have documentation.                            | Catalog         |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| undocumented_public_models           | | Identifies models in the dbt project           | Manifest        | None                      |
|                                      | | that are marked as public but don’t            |                 |                           |
|                                      | | have documentation.                            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+

4. Testing Insights
---------------------

+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| Name                                 | Description                                      | Files Required  | Overrides                 |
+======================================+==================================================+=================+===========================+
| missing_primary_key_tests            | | Identifies dbt models in the project           | Manifest        | None                      |
|                                      | | that lack primary key tests, which are         |                 |                           |
|                                      | | crucial for ensuring data integrity            |                 |                           |
|                                      | | and correctness.                               |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| dbt_low_test_coverage                | | Identifies dbt models in the project           | Manifest        | min_test_coverage_percent |
|                                      | | that have tests coverage percentage            |                 |                           |
|                                      | | below the required threshold.                  |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+

5. Project Structure Insights
----------------------------

+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| Name                                 | Description                                      | Files Required  | Overrides                 |
+======================================+==================================================+=================+===========================+
| model_directory_structure            | | Checks for correct placement of models         | Manifest        | None                      |
|                                      | | in their designated directories. Proper        |                 |                           |
|                                      | | directory structure is essential for ,         |                 |                           |
|                                      | | organization, discoverability, and maintenance |                 |                           |
|                                      | | within the dbt project.                        |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| model_naming_convention_check        | | Ensures all models adhere to a predefined      | Manifest        | None                      |
|                                      | | naming convention. A consistent naming         |                 |                           |
|                                      | | convention is crucial for clarity,             |                 |                           |
|                                      | | understanding of the model's purpose, and      |                 |                           |
|                                      | | enhancing navigation within the dbt project.   |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| source_directory_structure           | | Verifies if sources are correctly placed in    | Manifest        | None                      |
|                                      | | their designated directories. Proper directory |                 |                           |
|                                      | | placement for sources is important for         |                 |                           |
|                                      | | organizationand easy searchability.            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| test_directory_structure             | | Checks if tests are correctly placed in the    | Manifest        | None                      |
|                                      | | same directories as their corresponding models.|                 |                           |
|                                      | | Co locating tests with models aids in          |                 |                           |
|                                      | | maintainability and clarity.                   |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+

6. Check Insights
---------------------

+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| Name                                 | Description                                      | Files Required  | Overrides                 |
+======================================+==================================================+=================+===========================+
| column_descriptions_are_same         | | Checks if the column descriptions in the dbt   | Manifest        | None                      |
|                                      | | project are consistent across the project.     |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| column_name_contract                 | | Checks if the column names in the dbt project  | Manifest,       | None                      |
|                                      | | abide by the column name contract which        | Catalog         |                           |
|                                      | | consists of a regex pattern and a series       |                 |                           |
|                                      | | of data types.                                 |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_macro_args_have_desc           | | Checks if the macro arguments in the dbt       | Manifest        | None                      |
|                                      | | project have descriptions.                     |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_macro_has_desc                 | | Checks if the macros in the dbt project        | Manifest        | None                      |
|                                      | | have descriptions.                             |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_has_all_columns          | | Checks if the models in the dbt project        | Manifest,       | None                      |
|                                      | | have all the columns that are present in       | Catalog         |                           |
|                                      | | the data catalog.                              |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_has_valid_meta_keys      | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | have meta keys.                                |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_has_properties_file      | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | have a properties file.                        |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_has_tests_by_name        | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | have tests by name.                            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_has_tests_by_type        | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | have tests by type.                            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_has_tests_by_group       | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | have tests by group.                           |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_materialization_by_childs| | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | have materialization by a given threshold      |                 |                           |
|                                      | | of child models.                               |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| model_name_by_folder                 | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | abide by the model name contract which         |                 |                           |
|                                      | | consists of a regex pattern.                   |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_parents_and_childs       | | Checks if the model has min/max parents        | Manifest        | None                      |
|                                      | | and childs.                                    |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_parents_database         | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | has parent database in whitelist and           |                 |                           |
|                                      | | not in blacklist.                              |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_parents_schema           | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | has parent schema in whitelist and             |                 |                           |
|                                      | | not in blacklist.                              |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_model_tags                     | | Checks if the models in the dbt project        | Manifest        | None                      |
|                                      | | have tags in provided list of tags.            |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_childs                  | | Checks if the source has min/max childs        | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_columns_have_desc       | | Checks if the source columns have descriptions | Manifest,       | None                      |
|                                      | | in the dbt project.                            | Catalog         |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_all_columns         | | Checks if the source has all columns           | Manifest,       | None                      |
|                                      | | present in the data catalog.                   | Catalog         |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_freshness           | | Checks if the source has freshness             | Manifest        | None                      |
|                                      | | options.                                       |                 |                           |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_loader              | | Checks if the source has loader                | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_meta_keys           | | Checks if the source has meta keys             | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_tests_by_name       | | Checks if the source has tests by name         | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_tests_by_type       | | Checks if the source has tests by type         | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_tests_by_group      | | Checks if the source has tests by group        | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_has_tests               | | Checks if the source has tests                 | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_table_has_desc          | | Checks if the source table has description     | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
| check_source_tags                    | | Checks if the source has tags                  | Manifest        | None                      |
+--------------------------------------+--------------------------------------------------+-----------------+---------------------------+
