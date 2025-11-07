========
Overview
========

.. start-badges

.. |docs| image:: https://readthedocs.org/projects/datapilot/badge/?style=flat
    :target: https://datapilot.readthedocs.io/
    :alt: Documentation Status

.. |build| image:: https://github.com/AltimateAI/datapilot-cli/workflows/build/badge.svg
    :target: https://github.com/AltimateAI/datapilot-cli/actions
    :alt: Build Status

.. |codecov| image:: https://codecov.io/gh/anandgupta42/datapilot/branch/main/graphs/badge.svg?branch=main
    :alt: Coverage Status
    :target: https://app.codecov.io/github/anandgupta42/datapilot

.. |pypi| image:: https://badge.fury.io/py/altimate-datapilot-cli.svg
    :target: https://pypi.org/project/altimate-datapilot-cli/
    :alt: PyPI Package

.. |pyversion| image:: https://img.shields.io/pypi/pyversions/altimate-datapilot-cli.svg
    :target: https://pypi.org/project/altimate-datapilot-cli/
    :alt: Python Versions

.. |license| image:: https://img.shields.io/github/license/AltimateAI/datapilot-cli.svg
    :target: https://github.com/AltimateAI/datapilot-cli/blob/main/LICENSE
    :alt: License

.. |ruff| image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
    :target: https://github.com/astral-sh/ruff
    :alt: Ruff

.. |maintained| image:: https://img.shields.io/badge/Maintained%3F-yes-green.svg
    :target: https://github.com/AltimateAI/datapilot-cli/graphs/commit-activity
    :alt: Maintained

|docs| |build| |codecov| |pypi| |pyversion| |license| |ruff| |maintained|

.. end-badges

Assistant for Data Teams

* Free software: MIT license

Installation
============

::

    pip install altimate-datapilot-cli

You can also install the in-development version with::

    pip install https://github.com/AltimateAI/datapilot-cli/archive/main.zip


Documentation
=============


https://datapilot.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
