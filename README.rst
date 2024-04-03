========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |github-actions|
        | |codecov|
        | |scrutinizer|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/datapilot/badge/?style=flat
    :target: https://datapilot.readthedocs.io/
    :alt: Documentation Status

.. |github-actions| image:: https://github.com/AltimateAI/datapilot-cli/actions/workflows/github-actions.yml/badge.svg
    :alt: GitHub Actions Build Status
    :target: https://github.com/AltimateAI/datapilot/actions

.. |codecov| image:: https://codecov.io/gh/anandgupta42/datapilot/branch/main/graphs/badge.svg?branch=main
    :alt: Coverage Status
    :target: https://app.codecov.io/github/anandgupta42/datapilot




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
