"""Root pytest configuration."""
import sys
from pathlib import Path

# Add src directory to Python path for all tests
# This needs to run at import time, before pytest collects tests
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


def pytest_configure(config):
    """
    Hook that runs before test collection.
    Ensures src directory is in path before pytest imports test modules.
    """
    # Double-check src is in path
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
