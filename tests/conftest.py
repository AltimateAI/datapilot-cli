"""Root pytest configuration.

This file is required for tests to work in tox nocov mode (usedevelop=false).

In tox.ini, the usedevelop setting varies:
- cover mode: usedevelop=true (package installed in editable mode)
- nocov mode: usedevelop=false (package NOT installed)

When usedevelop=false, Python can't find the vendor module for imports like:
    from vendor.dbt_artifacts_parser.parsers.catalog.catalog_v1 import Metadata

This conftest.py adds src/ to sys.path so imports work in both modes.
"""
import sys
from pathlib import Path

# Add src directory to Python path for all tests
# This runs at import time, before pytest collects test modules
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


def pytest_configure(config):
    """
    Hook that runs before test collection.

    Ensures src directory is in sys.path before pytest imports test modules.
    This is a safety check in case the module-level code above didn't run.
    """
    # Double-check src is in path
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
