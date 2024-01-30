from datapilot.core.platforms.dbt.insights.dbt_test.missing_primary_key_tests import MissingPrimaryKeyTests
from datapilot.core.platforms.dbt.insights.dbt_test.test_coverage import DBTTestCoverage
from datapilot.core.platforms.dbt.insights.governance.documentation_on_stale_columns import DBTDocumentationStaleColumns
from datapilot.core.platforms.dbt.insights.governance.exposures_dependent_on_private_models import DBTExposureDependentOnPrivateModels
from datapilot.core.platforms.dbt.insights.governance.public_models_without_contracts import DBTPublicModelWithoutContracts
from datapilot.core.platforms.dbt.insights.governance.undocumented_columns import DBTMissingDocumentation
from datapilot.core.platforms.dbt.insights.governance.undocumented_public_models import DBTUndocumentedPublicModels
from datapilot.core.platforms.dbt.insights.modelling.direct_join_to_source import DBTDirectJoinSource
from datapilot.core.platforms.dbt.insights.modelling.downstream_models_dependent_on_source import DBTDownstreamModelsDependentOnSource
from datapilot.core.platforms.dbt.insights.modelling.duplicate_sources import DBTDuplicateSources
from datapilot.core.platforms.dbt.insights.modelling.hard_coded_references import DBTHardCodedReferences
from datapilot.core.platforms.dbt.insights.modelling.joining_of_upstream_concepts import DBTRejoiningOfUpstreamConcepts
from datapilot.core.platforms.dbt.insights.modelling.model_fanout import DBTModelFanout
from datapilot.core.platforms.dbt.insights.modelling.multiple_sources_joined import DBTModelsMultipleSourcesJoined
from datapilot.core.platforms.dbt.insights.modelling.root_model import DBTRootModel
from datapilot.core.platforms.dbt.insights.modelling.source_fanout import DBTSourceFanout
from datapilot.core.platforms.dbt.insights.modelling.staging_model_dependent_on_downstream_models import (
    DBTStagingModelsDependentOnDownstreamModels,
)
from datapilot.core.platforms.dbt.insights.modelling.staging_model_dependent_on_staging_models import (
    DBTStagingModelsDependentOnStagingModels,
)
from datapilot.core.platforms.dbt.insights.modelling.unused_sources import DBTUnusedSources
from datapilot.core.platforms.dbt.insights.performance.chain_view_linking import DBTChainViewLinking
from datapilot.core.platforms.dbt.insights.performance.exposure_parent_materializations import DBTExposureParentMaterialization
from datapilot.core.platforms.dbt.insights.structure.model_directories_structuire import DBTModelDirectoryStructure
from datapilot.core.platforms.dbt.insights.structure.model_naming_conventions import DBTModelNamingConvention
from datapilot.core.platforms.dbt.insights.structure.source_directories_structure import DBTSourceDirectoryStructure
from datapilot.core.platforms.dbt.insights.structure.test_directory_structure import DBTTestDirectoryStructure

INSIGHTS = [
    DBTDirectJoinSource,
    DBTDownstreamModelsDependentOnSource,
    DBTDuplicateSources,
    DBTModelFanout,
    DBTRootModel,
    DBTSourceFanout,
    DBTStagingModelsDependentOnDownstreamModels,
    DBTStagingModelsDependentOnStagingModels,
    DBTUnusedSources,
    DBTModelsMultipleSourcesJoined,
    DBTHardCodedReferences,
    DBTRejoiningOfUpstreamConcepts,
    DBTExposureDependentOnPrivateModels,
    DBTUndocumentedPublicModels,
    DBTPublicModelWithoutContracts,
    DBTChainViewLinking,
    DBTExposureParentMaterialization,
    DBTMissingDocumentation,
    DBTDocumentationStaleColumns,
    MissingPrimaryKeyTests,
    DBTTestCoverage,
    DBTModelDirectoryStructure,
    DBTModelNamingConvention,
    DBTSourceDirectoryStructure,
    DBTTestDirectoryStructure,
]
