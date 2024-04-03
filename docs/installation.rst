========================
Installation
========================

Prerequisites
=============

Before installing DataPilot, ensure you have the following prerequisites met:

- Python 3.7 or higher installed on your machine.
- Access to a command-line interface (CLI) to execute pip commands.
- An existing dbt project to analyze with DataPilot.

Installation
============

To install DataPilot, open your CLI and run the following command:

.. code-block:: shell

    pip install altimate-datapilot-cli

This command will download and install the latest version of DataPilot along with its dependencies.

QuickStart
==========

Once DataPilot is installed, you can set it up to work with your dbt project.

Execute the following command to perform a health check on your dbt project:

.. code-block:: shell

    datapilot dbt project-health --manifest-path /path/to/manifest.json --catalog-path /path/to/catalog.json

After running the command, DataPilot will provide you with insights into your dbt project's health. Review the insights and make any necessary adjustments to your project.
