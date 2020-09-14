cohortId <- 141933001
targetCohortIds <- cohortId
comparatorCohortIds <- 141933003
cohortIds <- c(targetCohortIds, comparatorCohortIds)
databaseId <- 'OPTUM_EXTENDED_DOD'
databaseIds <- c(databaseId, 'OPTUM_PANTHER')

connection = NULL
connectionDetails = NULL
resultsDatabaseSchema = NULL
library(magrittr)


data <- CohortDiagnostics::getTimeDistributionResult(cohortIds = cohortIds, 
                                                     databaseIds = databaseIds)
CohortDiagnostics::plotTimeDistribution(data = data, 
                                        cohortIds = c(141933001),
                                        databaseIds = databaseIds)



########################
data <- CohortDiagnostics::getIncidenceRateResult(cohortIds = cohortIds,
                                                  stratifyByAgeGroup = FALSE,
                                                  stratifyByGender = FALSE,
                                                  stratifyByCalendarYear = TRUE,
                                                  databaseIds = databaseIds)
CohortDiagnostics::plotIncidenceRate(data = data,
                                     stratifyByAgeGroup = FALSE,
                                     stratifyByGender = FALSE,
                                     stratifyByCalendarYear = TRUE)

data <- CohortDiagnostics::getIncidenceRateResult(cohortIds = cohortIds,
                                                  stratifyByAgeGroup = TRUE,
                                                  stratifyByGender = FALSE,
                                                  stratifyByCalendarYear = TRUE,
                                                  databaseIds = databaseIds)
CohortDiagnostics::plotIncidenceRate(data = data,
                                     stratifyByAgeGroup = TRUE,
                                     stratifyByGender = FALSE,
                                     stratifyByCalendarYear = TRUE)

#################
data <- CohortDiagnostics::getCohortCountResult(databaseIds = databaseIds)

########################
data <- CohortDiagnostics::getCohortOverLapResult(targetCohortIds = targetCohortIds,
                                                  comparatorCohortIds = comparatorCohortIds,
                                                  databaseIds = databaseIds)
CohortDiagnostics::plotCohortOverlapVennDiagram(data = data, 
                                                targetCohortIds = targetCohortIds,
                                                comparatorCohortIds = comparatorCohortIds,
                                                databaseIds = databaseId)
######################
covariateReference <- CohortDiagnostics::getCovariateReference(isTemporal = FALSE)
cohorts <- CohortDiagnostics::getCohortReference()
temporalCovariateReference <- CohortDiagnostics::getCovariateReference(isTemporal = TRUE)
timeReference <- CohortDiagnostics::getTimeReference()


data <- CohortDiagnostics::getCovariateValueResult(cohortIds = cohortIds, 
                                                   databaseIds = databaseIds, 
                                                   minProportion = 0.03)

data <- CohortDiagnostics::compareCovariateValueResult(targetCohortIds = targetCohortIds,
                                                       comparatorCohortIds = comparatorCohortIds,
                                                       databaseIds = databaseId,
                                                       isTemporal = FALSE,
                                                       minProportion = 0)

plot <- CohortDiagnostics::plotCohortComparisonStandardizedDifference(data = data,
                                                                      targetCohortIds = targetCohortIds, 
                                                                      comparatorCohortIds = comparatorCohortIds,
                                                                      cohort = cohorts,
                                                                      covariateReference = covariateReference,
                                                                      concept = NULL, # to subset based on domain, or vocabulary
                                                                      databaseIds = 'OPTUM_EXTENDED_DOD')

data <- CohortDiagnostics::getCovariateValueResult(cohortIds = cohortIds, 
                                                   databaseIds = databaseIds, 
                                                   minProportion = 0.03,
                                                   isTemporal = TRUE)

data <- CohortDiagnostics::getCovariateValueResult(cohortIds = cohortIds, 
                                                   databaseIds = databaseIds, 
                                                   minProportion = 0.03,
                                                   isTemporal = TRUE, 
                                                   timeIds = c(1,2,3))
