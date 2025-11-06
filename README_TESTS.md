# Running Tests

## Quick Start (Using Make)

The easiest way to run tests:

```bash
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
```

## Manual Test Commands

If you prefer to run pytest directly:

```bash
# Activate virtual environment
source .venv/bin/activate

# Run catalog extra fields tests
python -m pytest tests/test_vendor/test_catalog_v1.py -v
```

## All Test Commands

```bash
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
```

## Using tox (Recommended for full test suite)

The GitHub Actions CI uses tox to run tests across multiple Python and Pydantic versions:

```bash
# Run tests with Python 3.10 and Pydantic 2.10 (no coverage)
python3 -m tox -e py310-pydantic210-nocov

# Run tests with coverage
python3 -m tox -e py310-pydantic210-cover

# Run specific tests with tox
python3 -m tox -e py310-pydantic210-nocov -- tests/test_vendor/test_catalog_v1.py
```

## Continuous Integration

Tests run automatically on every push and pull request via GitHub Actions (`.github/workflows/github-actions.yml`).

The CI runs tests across:
- Python versions: 3.10, 3.11, 3.12, PyPy 3.10
- Pydantic versions: 2.8, 2.10
- With and without coverage reports
