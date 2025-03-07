dbt
===

project-health
--------------

The ``project-health`` feature in DataPilot is a comprehensive tool designed to analyze and report on various aspects of your dbt project. This feature is currently supported for dbt version 1.6 and 1.7.

How to Use
^^^^^^^^^^

To use the ``project-health`` feature, run the following command in your dbt project directory:

Step 1: Generate a manifest file for your dbt project.

.. code-block:: shell

    dbt compile

This command will generate a manifest file for your dbt project under the configured ``target`` directory. The default location for this directory is ``target/manifest.json``.

Step 2: Generate a catalog file for your dbt project.

.. code-block:: shell

    dbt docs generate

This command will generate a catalog file for your dbt project under the configured ``target`` directory. The default location for this directory is ``target/catalog.json``.

Step 3: Run the ``project-health`` command.

.. code-block:: shell

    datapilot dbt project-health --manifest-path ./target/manifest.json --catalog-path ./target/catalog.json


The catalog path is optional. If you do not provide a catalog path, the command will still run, but the catalog-related insights will not be available.

You can also select specific list of models to run the health check on by providing the '--select' flag. For example:

.. code-block:: shell

    datapilot dbt project-health --manifest-path ./target/manifest.json --select "path:dir1 path:dir2 model1 model2"

This will run the health check on all the models in the 'dir1' and 'dir2' directory, as well as the 'model1' and 'model2' models.
As of now, the '--select' flag only supports filtering based on model path and model name. We will add support for other filters and make it compatible
with the dbt commands soon.

3. **Configuration**:
You can provide configuration in two ways:

a. Using a local config file:
    .. code-block:: shell

        datapilot dbt project-health --manifest-path ./target/manifest.json --config-path ./path/to/config.yml

b. Using a named config from the API:
    .. code-block:: shell

        datapilot dbt project-health --manifest-path ./target/manifest.json --config-name "my-config" --token "YOUR_API_TOKEN" --instance-name "YOUR_INSTANCE"

The ``--config-name`` option allows you to use a configuration stored in the Altimate API. When using this option, you must also provide:
- ``--token``: Your API token for authentication
- ``--instance-name``: Your tenant ID

If both ``--config-path`` and ``--config-name`` are provided, the local config file (``--config-path``) takes precedence.
