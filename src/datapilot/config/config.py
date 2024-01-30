from configtree import Loader
from configtree.tree import Tree


def load_config(config_file_path: str) -> Tree:
    load = Loader()
    return load(config_file_path)


if __name__ == "__main__":
    config = load_config("/Users/surya/repos/altimate_dbt_package/tests/data/config.yml")
    print(config)
