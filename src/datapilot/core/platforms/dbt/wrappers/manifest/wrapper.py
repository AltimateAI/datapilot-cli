from abc import ABC, abstractmethod
from typing import Dict, Set, Text

from datapilot.core.platforms.dbt.schemas.manifest import (
    AltimateManifestExposureNode, AltimateManifestNode,
    AltimateManifestSourceNode, AltimateManifestTestNode)


class BaseManifestWrapper(ABC):
    @abstractmethod
    def get_nodes(self) -> Dict[Text, AltimateManifestNode]:
        pass

    @abstractmethod
    def get_package(self) -> str:
        pass

    @abstractmethod
    def get_sources(self) -> Dict[Text, AltimateManifestSourceNode]:
        pass

    @abstractmethod
    def get_exposures(self) -> Dict[Text, AltimateManifestExposureNode]:
        pass

    @abstractmethod
    def parent_to_child_map(self, nodes: Dict[Text, AltimateManifestNode]) -> Dict[Text, Set[Text]]:
        pass

    @abstractmethod
    def get_tests(self, types=None) -> Dict[Text, AltimateManifestTestNode]:
        pass
