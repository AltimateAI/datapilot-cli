from pathlib import Path
from typing import Dict

from ruamel.yaml import YAML


def load_config(config_file_path: str) -> Dict:
    yaml = YAML(typ="safe")
    with Path(config_file_path).open() as file:
        config_dict = yaml.load(file)
    return config_dict


if __name__ == "__main__":
    config = load_config("/Users/surya/repos/altimate_dbt_package/tests/data/config.yml")
    print(config)
