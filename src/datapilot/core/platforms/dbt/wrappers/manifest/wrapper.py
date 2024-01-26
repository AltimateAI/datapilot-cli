from abc import ABC
from typing import Dict, Set, Text

from datapilot.core.platforms.dbt.schemas.manifest import (
    AltimateManifestExposureNode,
    AltimateManifestNode,
    AltimateManifestSourceNode,
    AltimateManifestTestNode,
)


class BaseManifestWrapper(ABC):
    def get_nodes(self) -> Dict[Text, AltimateManifestNode]:
        pass

    def get_package(self) -> str:
        pass

    def get_sources(self) -> Dict[Text, AltimateManifestSourceNode]:
        pass

    def get_exposures(self) -> Dict[Text, AltimateManifestExposureNode]:
        pass

    def parent_to_child_map(self, nodes: Dict[Text, AltimateManifestNode]) -> Dict[Text, Set[Text]]:
        pass

    def get_tests(self, types=None) -> Dict[Text, AltimateManifestTestNode]:
        pass
