import re
from enum import Enum
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from dbt_artifacts_parser.parser import parse_catalog
from dbt_artifacts_parser.parser import parse_manifest

from datapilot.core.platforms.dbt.constants import BASE
from datapilot.core.platforms.dbt.constants import FOLDER
from datapilot.core.platforms.dbt.constants import INTERMEDIATE
from datapilot.core.platforms.dbt.constants import MART
from datapilot.core.platforms.dbt.constants import MODEL
from datapilot.core.platforms.dbt.constants import OTHER
from datapilot.core.platforms.dbt.constants import STAGING
from datapilot.core.platforms.dbt.exceptions import AltimateInvalidManifestError
from datapilot.core.platforms.dbt.factory import DBTFactory
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestExposureNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestSourceNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestTestNode
from datapilot.core.platforms.dbt.schemas.manifest import Catalog
from datapilot.core.platforms.dbt.schemas.manifest import Manifest
from datapilot.exceptions.exceptions import AltimateFileNotFoundError
from datapilot.exceptions.exceptions import AltimateInvalidJSONError
from datapilot.utils.utils import extract_dir_name_from_file_path
from datapilot.utils.utils import extract_folders_in_path
from datapilot.utils.utils import is_superset_path
from datapilot.utils.utils import load_json

MODEL_TYPE_PATTERNS = {
    STAGING: r"^stg_.*",  # Example: models starting with 'stg_'
    MART: r"^(mrt_|mart_|fct_|dim_).*",  # Example: models starting with 'mrt_' or 'mart_'
    INTERMEDIATE: r"^int_.*",  # Example: models starting with 'int_'
    BASE: r"^base_.*",  # Example: models starting with 'base_'
    # Add other model types with their regex patterns here
}

FOLDER_MAP = {
    STAGING: STAGING,
    MART: MART,
    INTERMEDIATE: INTERMEDIATE,
    BASE: BASE,
    # Add other model types with their folder names here
}


class SelectOption(Enum):
    DIRECTORY = "directory"
    MODEL_NAME = "model_name"
    MODEL_PATH = "model_path"


def combine_dict(dict1: Dict, dict2: Optional[Dict]) -> Dict:
    dict2 = dict2 or {}
    return {**dict1, **dict2}


def load_manifest(manifest_path: str) -> Manifest:
    try:
        manifest_dict = load_json(manifest_path)
    except FileNotFoundError as e:
        raise AltimateFileNotFoundError(f"Manifest file not found: {manifest_path}. Error: {e}") from e
    except ValueError as e:
        raise AltimateInvalidJSONError(f"Invalid manifest file: {manifest_path}. Error: {e}") from e
    except Exception as e:
        raise AltimateInvalidManifestError(
            f"Invalid manifest file: {manifest_path}. Error: {e}. Please ensure that you are providing the path to a manifest file"
        ) from e

    try:
        manifest: Manifest = parse_manifest(manifest_dict)
    except ValueError as e:
        raise AltimateInvalidManifestError(f"Invalid manifest file: {manifest_path}. Error: {e}") from e

    return manifest


def load_catalog(catalog_path: str) -> Catalog:
    try:
        catalog_dict = load_json(catalog_path)
    except FileNotFoundError as e:
        raise AltimateFileNotFoundError(f"Manifest file not found: {catalog_path}. Error: {e}") from e
    except ValueError as e:
        raise AltimateInvalidJSONError(f"Invalid JSON file: {catalog_path}. Error: {e}") from e

    try:
        catalog: Catalog = parse_catalog(catalog_dict)
    except ValueError as e:
        raise AltimateInvalidManifestError(f"Invalid manifest file: {catalog_path}. Error: {e}") from e

    return catalog


def load_run_results(run_results_path: str) -> Manifest:
    raise NotImplementedError


# TODO: Add tests!
def get_table_name_from_source(source: AltimateManifestSourceNode) -> str:
    db = source.database
    schema = source.schema_name
    identifier = source.identifier
    if db:
        return f"{db}.{schema}.{identifier}"
    return f"{schema}.{identifier}"


def classify_model_type_by_name(
    model_name: str,
    model_name_pattern: Optional[Dict[str, str]],
):
    types_patterns = combine_dict(MODEL_TYPE_PATTERNS, model_name_pattern)
    for model_type, pattern in types_patterns.items():
        if re.match(pattern, model_name):
            return model_type

    return None


def classify_model_type_by_folder(model_path: str, model_folder_pattern: Optional[Dict[str, str]]) -> str:
    folder_patterns = combine_dict(FOLDER_MAP, model_folder_pattern)
    dirname = extract_dir_name_from_file_path(model_path)
    for model_type, pattern in folder_patterns.items():
        if re.match(pattern, dirname):
            return model_type

    return OTHER


# TODO: Add tests!
def classify_model_type(
    model_name: str,
    folder_path: Optional[str] = None,
    patterns: Optional[Dict[str, Optional[Dict[str, str]]]] = None,
) -> Optional[str]:
    """
    Classify the type of a model based on its name using regex patterns.

    :param model_name: The name of the model.
    :param types_patterns: A dictionary mapping model types to their regex patterns.
    :return: The type of the model or None if no match is found.
    """
    type_patterns = patterns.get(MODEL, {})
    model_type = classify_model_type_by_name(model_name, type_patterns)

    if model_type:
        return model_type

    if folder_path:
        folder_patterns = patterns.get(FOLDER, {})
        model_type = classify_model_type_by_folder(folder_path, folder_patterns)
        if model_type:
            return model_type
    return OTHER  # if no pattern matches


def _check_model_naming_convention(
    model_name: str, expected_model_type: str, patterns: Optional[Dict[str, str]]
) -> Tuple[bool, Optional[str]]:
    model_patterns = combine_dict(MODEL_TYPE_PATTERNS, patterns)
    expected_model_pattern = model_patterns.get(expected_model_type)
    if expected_model_pattern:
        if re.match(expected_model_pattern, model_name):
            return True, None
    return False, expected_model_pattern


def get_node_source_name(
    node: AltimateManifestNode,
    sources: Dict[str, AltimateManifestSourceNode],
) -> str:
    for node_id in node.depends_on.nodes:
        if node_id in sources:
            return sources[node_id].source_name


def _check_mart_convention(folder_patterns, directory_name, node_name):
    if re.match(folder_patterns.get(MART, ""), directory_name):
        return True, None
    return (
        False,
        f"*/{folder_patterns.get(MART, '')}/{node_name}.sql",
    )


def _staging_error_message(source_name, node_name, staging_pattern):
    return f"*/{staging_pattern}/{source_name}/{node_name}.sql"


def _check_staging_convention(folder_path, folder_patterns, directory_name, node, sources):
    directories = extract_folders_in_path(folder_path)
    source_name = get_node_source_name(node, sources)
    if not source_name:
        return True, None
    if directory_name != source_name:
        return False, _staging_error_message(source_name, node.name, folder_patterns.get(STAGING, ""))

    staging_pattern = folder_patterns.get(STAGING)
    if staging_pattern and len(directories) > 2 and not re.match(staging_pattern, directories[-2]):
        return False, _staging_error_message(source_name, node.name, staging_pattern)

    return True, None


def _check_source_folder_convention(source_name, folder_path, patterns=Optional[Dict[str, Dict[str, str]]]):
    folder_patterns = combine_dict(FOLDER_MAP, patterns.get(FOLDER))
    directories = extract_folders_in_path(folder_path)
    directory_name = extract_dir_name_from_file_path(folder_path)
    if directory_name != source_name:
        return False, f"{folder_patterns.get(STAGING)}/{source_name}/source.yml"

    if len(directories) > 2 and not re.match(folder_patterns.get(STAGING), directories[-2]):
        return False, f"{folder_patterns.get(STAGING)}/{source_name}/source.yml"

    return True, None


def _check_model_folder_convention(
    model_type: str,
    folder_path: str,
    patterns: Dict[str, Optional[Dict[str, str]]],
    node: AltimateManifestNode,
    sources: Dict[str, AltimateManifestSourceNode],
) -> Tuple[bool, Optional[str]]:
    folder_patterns = patterns.get(FOLDER, {}) or {}
    folder_patterns = {**FOLDER_MAP, **folder_patterns}
    directory_name = extract_dir_name_from_file_path(folder_path)
    if model_type == MART:
        return _check_mart_convention(folder_patterns, directory_name, node.name)

    if model_type == STAGING:
        return _check_staging_convention(folder_path, folder_patterns, directory_name, node, sources)

    return True, None


# TODO: Add tests!
def get_children_map(nodes: Dict[str, AltimateManifestNode]) -> Dict[str, AltimateManifestNode]:
    """
    Current manifest contains information about parents
    THis gives an information of node to children

    :param nodes: A dictionary of nodes in a manifest.
    :return: A dictionary of all the children of a node.
    """
    children_map = {}
    for node_id, node in nodes.items():
        for parent in node.depends_on.nodes:
            children_map.setdefault(parent, set()).add(node_id)
    return children_map


# TODO: Add tests!
def get_hard_coded_references(sql_code):
    """
    Find all hard-coded references in the given SQL code.

    :param sql_code: A string containing the SQL code to be analyzed.
    :return: A set of unique hard-coded references found in the SQL code.
    """
    # Define regex patterns to match different types of hard-coded references
    from_hard_coded_references = {
        "from_var_1": r"""(?ix)

                    # first matching group
                    # from or join followed by at least 1 whitespace character
                        (from | join)\s +

                         # second matching group
                         # opening {{, 0 or more whitespace character(s), var, 0 or more whitespace character(s), an opening parenthesis, 0 or more whitespace character(s), 1 or 0 quotation mark
                         ({{\s * var\s * \(\s *[\'\"]?)

                         # third matching group
                         # at least 1 of anything except a parenthesis or quotation mark
                         ([^)\'\"]+)

            # fourth matching group
            # 1 or 0 quotation mark, 0 or more whitespace character(s)
                ([\'\"]?\s*)

            # fifth matching group
            # a closing parenthesis, 0 or more whitespace character(s), closing }}
                (\)\s *}})

    """,
        "from_var_2": r"""(?ix)

    # first matching group
    # from or join followed by at least 1 whitespace character
        (
    from | join)\s +

                 # second matching group
                 # opening {{, 0 or more whitespace character(s), var, 0 or more whitespace character(s), an opening parenthesis, 0 or more whitespace character(s), 1 or 0 quotation mark
                 ({{\s * var\s * \(\s *[\'\"]?)

                 # third matching group
                 # at least 1 of anything except a parenthesis or quotation mark
                 ([^)\'\"]+)

    # fourth matching group
    # 1 or 0 quotation mark, 0 or more whitespace character(s)
        ([\'\"]?\s*)

    # fifth matching group
    # a comma
        (,)

    # sixth matching group
    # 0 or more whitespace character(s), 1 or 0 quotation mark
    (\s *[\'\"]?)

     # seventh matching group
     # at least 1 of anything except a parenthesis or quotation mark
     ([^)\'\"]+)

    # eighth matching group
    # 1 or 0 quotation mark, 0 or more whitespace character(s)
        ([\'\"]?\s*)

    # ninth matching group
    # a closing parenthesis, 0 or more whitespace character(s), closing }}
        (\)\s *}})

    """,
        "from_table_1": r"""(?ix)

    # first matching group
    # from or join followed by at least 1 whitespace character
        (
    from | join)\s +

                 # second matching group
                 # 1 or 0 of (opening bracket, backtick, or quotation mark)
                 ([\[`\"\']?)

                 # third matching group
                 # at least 1 word character
                 (\w+)

                 # fouth matching group
                 # 1 or 0 of (closing bracket, backtick, or quotation mark)
                     ([\]`\"\']?)

    # fifth matching group
    # a period
        (\.)

    # sixth matching group
    # 1 or 0 of (opening bracket, backtick, or quotation mark)
    ([\[`\"\']?)

    # seventh matching group
    # at least 1 word character
    (\w+)

    # eighth matching group
    # 1 or 0 of (closing bracket, backtick, or quotation mark) folowed by a whitespace character or end of string
        ([\]`\"\']?)(?=\s|$)

              """,
        "from_table_2": r"""(?ix)

    # first matching group
    # from or join followed by at least 1 whitespace character
        (
    from | join)\s +

                 # second matching group
                 # 1 or 0 of (opening bracket, backtick, or quotation mark)
                 ([\[`\"\']?)

                 # third matching group
                 # at least 1 word character
                 (\w+)
                 # fouth matching group
                 # 1 or 0 of (closing bracket, backtick, or quotation mark)
                     ([\]`\"\']?)

    # fifth matching group
    # a period
        (\.)

    # sixth matching group
    # 1 or 0 of (opening bracket, backtick, or quotation mark)
    ([\[`\"\']?)

    # seventh matching group
    # at least 1 word character
    (\w+)

    # eighth matching group
    # 1 or 0 of (closing bracket, backtick, or quotation mark)
        ([\]`\"\']?)

    # ninth matching group
    # a period
        (\.)

    # tenth matching group
    # 1 or 0 of (closing bracket, backtick, or quotation mark)
    ([\[`\"\']?)

    # eleventh matching group
    # at least 1 word character
    (\w+)

    # twelfth matching group
    # 1 or 0 of (closing bracket, backtick, or quotation mark) folowed by a whitespace character or end of string
        ([\]`\"\']?)(?=\s|$)

              """,
        "from_table_3": r"""(?ix)

    # first matching group
    # from or join followed by at least 1 whitespace character
        (
    from | join)\s +

                 # second matching group
                 # 1 of (opening bracket, backtick, or quotation mark)
                 ([\[`\"\'])

                 # third matching group
                 # at least 1 word character or space
                 ([\w]+)

                 # fourth matching group
                 # 1 of (closing bracket, backtick, or quotation mark) folowed by a whitespace character or end of string
                     ([\]`\"\'])(?=\s|$)

                           """,
    }

    # Set to store all unique hard-coded references
    hard_coded_references = set()
    for regex_pattern in from_hard_coded_references.values():
        # Compile the regex pattern
        all_regex_matches = re.findall(regex_pattern, sql_code)

        # Find all matches in the SQL code
        # Process each match
        for match in all_regex_matches:
            # Extract all groups except the first one and join them
            raw_reference = "".join(match[1:]).strip()  #

            hard_coded_references.add(raw_reference)
    return hard_coded_references


def parse_argument(argument: str) -> dict:
    """
    Parses the given argument to categorize it as a model path, directory, or model name.

    Parameters:
    - argument (str): The input argument to be parsed.

    Returns:
    - dict: A dictionary containing the 'type' and 'name' of the parsed argument.
    """
    # Determine if the argument is a model path or directory based on its prefix and suffix.
    if argument.startswith("path:"):
        path_type = SelectOption.MODEL_PATH if argument.endswith(".sql") else SelectOption.DIRECTORY
        path = argument.split(":", 1)[1]
        return {"type": path_type, "name": path}

    # Identify argument as a model path if it ends with '.sql'.
    if argument.endswith(".sql"):
        return {"type": SelectOption.MODEL_PATH, "name": argument}

    # Identify argument as a directory if it contains path separators.
    if "/" in argument or "\\" in argument:
        return {"type": SelectOption.DIRECTORY, "name": argument}

    # Default case: treat the argument as a model name.
    return {"type": SelectOption.MODEL_NAME, "name": argument}


def add_models_by_type(selected_category: dict, entities: dict, final_models: List[str]):
    """
    Adds models to the final list based on the selected category.

    Parameters:
    - selected_category (dict): The category selected for adding models.
    - entities (dict): A dictionary of entities, each associated with a type.
    - final_models (List[str]): The list to which the models' unique IDs are added.
    """
    for entity in entities.values():
        if selected_category["type"] in (SelectOption.MODEL_NAME, SelectOption.MODEL_PATH):
            if entity.name == selected_category.get("name") or entity.original_file_path == selected_category.get("name"):
                final_models.append(entity.unique_id)
        elif selected_category["type"] == SelectOption.DIRECTORY:
            if is_superset_path(selected_category["name"], entity.original_file_path):
                final_models.append(entity.unique_id)


def get_models(
    selected_model_list: Optional[List[str]],
    entities: Dict[str, Union[AltimateManifestNode, AltimateManifestExposureNode, AltimateManifestSourceNode, AltimateManifestTestNode]],
) -> List[str]:
    """
    Retrieves models based on a selected list and entities.

    Parameters:
    - selected_model_list (Optional[List[str]]): The list of selected models.
    - entities (Dict): A dictionary containing entity types and their instances.

    Returns:
    - List[str]: A list of unique model IDs based on the selection criteria.
    """
    final_models = []
    for selected_model in selected_model_list or []:
        selected_category = parse_argument(selected_model)
        for entity_type in entities:
            add_models_by_type(selected_category, entities[entity_type], final_models)
    return list(set(final_models))


def get_manifest_wrapper(manifest_path: str):
    manifest = load_manifest(manifest_path)
    return DBTFactory.get_manifest_wrapper(manifest)
