library(StudyDiagnostics)
source("S:/MiscCode/SetEnvironmentVariables.R")
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
                                                                server = Sys.getenv("server"),
                                                                user = NULL,
                                                                password = NULL,
                                                                port = Sys.getenv("port"))


# ischemic stroke --------------------------------------------------------------
cdmDatabaseSchema <- "cdm_ibm_ccae_v1061.dbo"
cohortDatabaseSchema <- "cdm_ibm_ccae_v1061.ohdsi_results"
cohortTable <- "cohort"
cohortDefinitionId <- 13397 # LEGEND ischemic stroke
workFolder <- file.path("S:/StudyResults/StudyDiagnostics/IschemicStroke")

ipDataFirst <- GetIncidenceProportionData(connectionDetails = connectionDetails,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortTable = cohortTable,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          firstOccurrenceOnly = TRUE,
                                          minObservationTime = 365,
                                          cohortDefinitionId = cohortDefinitionId,
                                          workFolder = workFolder)
ipDataFirst[[1]]
# NUM_COUNT DENOM_COUNT  IP_1000P
# 1    260154   364785821 0.7131692

ipPlots <- GenerateStabilityPlots(ipData = ipDataFirst,
                                  panel = "age", # "gender",
                                  workFolder = workFolder,
                                  restrictToFullAgeData = TRUE)

ipDataAll <- GetIncidenceProportionData(connectionDetails = connectionDetails,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        firstOccurrenceOnly = FALSE,
                                        minObservationTime = 365,
                                        cohortDefinitionId = cohortDefinitionId,
                                        workFolder = workFolder)
ipDataAll[[1]]
# NUM_COUNT DENOM_COUNT  IP_1000P
# 1    305781   365298156 0.8370724


# cardiac arrhythmia -----------------------------------------------------------
cdmDatabaseSchema <- "CDM_IBM_MDCR_V1062.dbo"
cohortDatabaseSchema <- "CDM_IBM_MDCR_V1062.ohdsi_results"
cohortTable <- "cohort"
cohortDefinitionId <-  7399 # LEGEND Cardiac Arrhythmia
workFolder <- file.path("S:/StudyResults/StudyDiagnostics/CardiacArrhythmia")

ipDataFirst <- GetIncidenceProportionData(connectionDetails = connectionDetails,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortTable = cohortTable,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          firstOccurrenceOnly = TRUE,
                                          minObservationTime = 365,
                                          cohortDefinitionId = cohortDefinitionId,
                                          workFolder = workFolder)

ipDataFirst[[1]]
# NUM_COUNT DENOM_COUNT IP_1000P
# 1   2423335    31501374 76.92791

ipPlots <- GenerateStabilityPlots(ipData = ipDataFirst,
                                  panel = "age", # "gender",
                                  workFolder = workFolder,
                                  restrictToFullAgeData = TRUE)


# angioedema -------------------------------------------------------------------
cdmDatabaseSchema <- "cdm_optum_extended_dod_v1064.dbo"
cohortDatabaseSchema <- "cdm_optum_extended_dod_v1064.ohdsi_results"
cohortTable <- "cohort"
cohortDefinitionId <- 7377 # LEGEND angioedema
workFolder <- file.path("S:/StudyResults/StudyDiagnostics/Angioedema")

ipDataFirst <- GetIncidenceProportionData(connectionDetails = connectionDetails,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortTable = cohortTable,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          firstOccurrenceOnly = TRUE,
                                          minObservationTime = 365,
                                          cohortDefinitionId = cohortDefinitionId,
                                          workFolder = workFolder)

ipDataFirst[[1]]
# NUM_COUNT DENOM_COUNT  IP_1000P
# 1     98384   278817850 0.3528612

ipPlots <- GenerateStabilityPlots(ipData = ipDataFirst,
                                  panel = "age", # "gender",
                                  workFolder = workFolder,
                                  restrictToFullAgeData = TRUE)

ipDataAll <- GetIncidenceProportionData(connectionDetails = connectionDetails,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        firstOccurrenceOnly = FALSE,
                                        minObservationTime = 365,
                                        cohortDefinitionId = cohortDefinitionId,
                                        workFolder = workFolder)

ipDataAll[[1]]
# NUM_COUNT DENOM_COUNT  IP_1000P
# 1    107007   279092991 0.3834098

ipPlots <- GenerateStabilityPlots(ipData = ipDataAll,
                                  panel = "age", # "gender",
                                  workFolder = NULL,
                                  restrictToFullAgeData = TRUE)


