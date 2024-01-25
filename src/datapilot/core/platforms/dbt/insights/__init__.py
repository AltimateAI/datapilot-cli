from datapilot.core.platforms.dbt.insights.dbt_test import (
    DBTTestCoverage, MissingPrimaryKeyTests)
from datapilot.core.platforms.dbt.insights.governance import (
    DBTDocumentationStaleColumns, DBTExposureDependentOnPrivateModels,
    DBTMissingDocumentation, DBTPublicModelWithoutContracts,
    DBTUndocumentedPublicModels)
from datapilot.core.platforms.dbt.insights.modelling import (
    DBTDirectJoinSource, DBTDownstreamModelsDependentOnSource,
    DBTDuplicateSources, DBTHardCodedReferences, DBTModelFanout,
    DBTModelsMultipleSourcesJoined, DBTRejoiningOfUpstreamConcepts,
    DBTRootModel, DBTSourceFanout, DBTStagingModelsDependentOnDownstreamModels,
    DBTStagingModelsDependentOnStagingModels, DBTUnusedSources)
from datapilot.core.platforms.dbt.insights.performance import (
    DBTChainViewLinking, DBTExposureParentMaterialization)
from datapilot.core.platforms.dbt.insights.structure import (
    DBTModelDirectoryStructure, DBTModelNamingConvention,
    DBTSourceDirectoryStructure, DBTTestDirectoryStructure)

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
