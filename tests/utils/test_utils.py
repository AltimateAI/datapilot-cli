from pathlib import Path

import pytest

from datapilot.utils.utils import extract_folders_in_path

test_cases = [
    (Path("/home/user/documents/file.txt"), ["home", "user", "documents"]),
    (Path("/home/user/documents/"), ["home", "user", "documents"]),
    (Path("/home/user/documents"), ["home", "user", "documents"]),
    (Path(), []),
    (Path("/"), []),
    (Path("file.txt"), []),
]


@pytest.mark.parametrize(("input_path", "expected"), test_cases)
def test_extract_folders_in_path(input_path, expected):
    assert extract_folders_in_path(str(input_path)) == expected
