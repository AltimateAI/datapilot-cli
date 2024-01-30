from datapilot.core.platforms.dbt.insights.dbt_test import DBTTestCoverage
from datapilot.core.platforms.dbt.insights.dbt_test import MissingPrimaryKeyTests
from datapilot.core.platforms.dbt.insights.governance import DBTDocumentationStaleColumns
from datapilot.core.platforms.dbt.insights.governance import DBTExposureDependentOnPrivateModels
from datapilot.core.platforms.dbt.insights.governance import DBTMissingDocumentation
from datapilot.core.platforms.dbt.insights.governance import DBTPublicModelWithoutContracts
from datapilot.core.platforms.dbt.insights.governance import DBTUndocumentedPublicModels
from datapilot.core.platforms.dbt.insights.modelling import DBTDirectJoinSource
from datapilot.core.platforms.dbt.insights.modelling import DBTDownstreamModelsDependentOnSource
from datapilot.core.platforms.dbt.insights.modelling import DBTDuplicateSources
from datapilot.core.platforms.dbt.insights.modelling import DBTHardCodedReferences
from datapilot.core.platforms.dbt.insights.modelling import DBTModelFanout
from datapilot.core.platforms.dbt.insights.modelling import DBTModelsMultipleSourcesJoined
from datapilot.core.platforms.dbt.insights.modelling import DBTRejoiningOfUpstreamConcepts
from datapilot.core.platforms.dbt.insights.modelling import DBTRootModel
from datapilot.core.platforms.dbt.insights.modelling import DBTSourceFanout
from datapilot.core.platforms.dbt.insights.modelling import DBTStagingModelsDependentOnDownstreamModels
from datapilot.core.platforms.dbt.insights.modelling import DBTStagingModelsDependentOnStagingModels
from datapilot.core.platforms.dbt.insights.modelling import DBTUnusedSources
from datapilot.core.platforms.dbt.insights.performance import DBTChainViewLinking
from datapilot.core.platforms.dbt.insights.performance import DBTExposureParentMaterialization
from datapilot.core.platforms.dbt.insights.structure import DBTModelDirectoryStructure
from datapilot.core.platforms.dbt.insights.structure import DBTModelNamingConvention
from datapilot.core.platforms.dbt.insights.structure import DBTSourceDirectoryStructure
from datapilot.core.platforms.dbt.insights.structure import DBTTestDirectoryStructure

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
