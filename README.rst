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

.. |github-actions| image:: https://github.com/AltimateAI/datapilot/actions/workflows/github-actions.yml/badge.svg
    :alt: GitHub Actions Build Status
    :target: https://github.com/AltimateAI/datapilot/actions

.. |codecov| image:: https://codecov.io/gh/anandgupta42/datapilot/branch/main/graphs/badge.svg?branch=main
    :alt: Coverage Status
    :target: https://app.codecov.io/github/anandgupta42/datapilot

.. |version| image:: https://img.shields.io/pypi/v/datapilot.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/datapilot

.. |wheel| image:: https://img.shields.io/pypi/wheel/datapilot.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/datapilot

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/datapilot.svg
    :alt: Supported versions
    :target: https://pypi.org/project/datapilot

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/datapilot.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/datapilot

.. |commits-since| image:: https://img.shields.io/github/commits-since/anandgupta42/datapilot/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/AltimateAI/datapilot/compare/v0.0.0...main


.. |scrutinizer| image:: https://img.shields.io/scrutinizer/quality/g/anandgupta42/datapilot/main.svg
    :alt: Scrutinizer Status
    :target: https://scrutinizer-ci.com/g/anandgupta42/datapilot/


.. end-badges

Assistant for Data Teams

* Free software: MIT license

Installation
============

::

    pip install datapilot

You can also install the in-development version with::

    pip install https://github.com/AltimateAI/datapilot/archive/main.zip


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
