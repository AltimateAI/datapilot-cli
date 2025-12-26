from typing import Union

from pydantic import ConfigDict

from vendor.dbt_artifacts_parser.parsers.semantic_manifest.semantic_manifest_v1 import SemanticManifestV1 as BaseSemanticManifestV1


class SemanticManifestV1(BaseSemanticManifestV1):
    model_config = ConfigDict(extra="allow")


SemanticManifest = Union[SemanticManifestV1]
