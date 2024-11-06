from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import Optional
from typing import Set

from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestExposureNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestSourceNode
from datapilot.core.platforms.dbt.schemas.manifest import AltimateManifestTestNode


class BaseManifestWrapper(ABC):
    @abstractmethod
    def get_nodes(self) -> Dict[str, AltimateManifestNode]:
        pass

    @abstractmethod
    def get_package(self) -> str:
        pass

    @abstractmethod
    def get_sources(self) -> Dict[str, AltimateManifestSourceNode]:
        pass

    @abstractmethod
    def get_exposures(self) -> Dict[str, AltimateManifestExposureNode]:
        pass

    @abstractmethod
    def get_adapter_type(self) -> Optional[str]:
        pass

    @abstractmethod
    def parent_to_child_map(self, nodes: Dict[str, AltimateManifestNode]) -> Dict[str, Set[str]]:
        pass

    @abstractmethod
    def get_tests(self, types=None) -> Dict[str, AltimateManifestTestNode]:
        pass
