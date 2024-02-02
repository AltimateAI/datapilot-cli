dbt
===

project-health
--------------

The ``project-health`` feature in DataPilot is a comprehensive tool designed to analyze and report on various aspects of your dbt project.

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
