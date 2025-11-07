============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

Bug reports
===========

When `reporting a bug <https://github.com/AltimateAI/datapilot/issues>`_ please include:

    * Your operating system name and version.
    * Any details about your local setup that might be helpful in troubleshooting.
    * Detailed steps to reproduce the bug.

Documentation improvements
==========================

datapilot could always use more documentation, whether as part of the
official datapilot docs, in docstrings, or even on the web in blog posts,
articles, and such.

Feature requests and feedback
=============================

The best way to send feedback is to file an issue at https://github.com/AltimateAI/datapilot/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that code contributions are welcome :)

Development
===========

To set up `datapilot` for local development:

1. Fork `datapilot <https://github.com/AltimateAI/datapilot-cli>`_
   (look for the "Fork" button).
2. Clone your fork locally::

    git clone git@github.com:YOURGITHUBNAME/datapilot.git

3. Create a branch for local development::

    git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

4. When you're done making changes run all the checks and docs builder with one command::

    tox

5. Commit your changes and push your branch to GitHub::

    git add .
    git commit -m "Your detailed description of your changes."
    git push origin name-of-your-bugfix-or-feature

6. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

If you need some code review or feedback while you're developing the code just make the pull request.

For merging, you should:

1. Include passing tests (run ``tox``).
2. Update documentation when there's new API, functionality etc.
3. Add a note to ``CHANGELOG.rst`` about the changes.
4. Add yourself to ``AUTHORS.rst``.

Running Tests
-------------

Quick Start (Using Make)
~~~~~~~~~~~~~~~~~~~~~~~~

The easiest way to run tests::

    # First time setup - create virtual environment
    make venv
    source .venv/bin/activate

    # Install dependencies
    make install

    # Run all tests
    make test

    # Run just the catalog vendor tests
    make test-vendor

    # Run tests with coverage
    make test-cov

Manual Test Commands
~~~~~~~~~~~~~~~~~~~~

If you prefer to run pytest directly::

    # Activate virtual environment
    source .venv/bin/activate

    # Run catalog extra fields tests
    python -m pytest tests/test_vendor/test_catalog_v1.py -v

All Test Commands
~~~~~~~~~~~~~~~~~

::

    # Run all catalog vendor tests
    python -m pytest tests/test_vendor/ -v

    # Run specific test file
    python -m pytest tests/test_vendor/test_catalog_v1.py -v

    # Run specific test class
    python -m pytest tests/test_vendor/test_catalog_v1.py::TestMetadataExtraFields -v

    # Run specific test method
    python -m pytest tests/test_vendor/test_catalog_v1.py::TestMetadataExtraFields::test_metadata_accepts_extra_fields -v

    # Run with more verbose output
    python -m pytest tests/test_vendor/test_catalog_v1.py -vv

    # Run and show print statements
    python -m pytest tests/test_vendor/test_catalog_v1.py -v -s

    # Run all tests in the project
    python -m pytest tests/ -v

Using tox
~~~~~~~~~

The GitHub Actions CI uses tox to run tests across multiple Python and Pydantic versions::

    # Run tests with Python 3.10 and Pydantic 2.10 (no coverage)
    python3 -m tox -e py310-pydantic210-nocov

    # Run tests with coverage
    python3 -m tox -e py310-pydantic210-cover

    # Run specific tests with tox
    python3 -m tox -e py310-pydantic210-nocov -- tests/test_vendor/test_catalog_v1.py

To run a subset of tests::

    tox -e envname -- pytest -k test_myfeature

To run all the test environments in *parallel*::

    tox -p auto

Continuous Integration
~~~~~~~~~~~~~~~~~~~~~~

Tests run automatically on every push and pull request via GitHub Actions (``.github/workflows/github-actions.yml``).

The CI runs tests across:

* Python versions: 3.10, 3.11, 3.12, PyPy 3.9
* Pydantic versions: 2.8, 2.10
* With and without coverage reports
