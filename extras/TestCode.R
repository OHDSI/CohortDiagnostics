library(StudyDiagnostics)

connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"),
                                             schema = "CDM_Truven_MDCR_V415")

cdmDatabaseSchema <- "CDM_IBM_MDCR_V1062.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "mschuemi_temp"
oracleTempSchema <- NULL

baseUrl <- Sys.getenv("baseUrl")
cohortId <- 7399 # LEGEND Cardiac Arrhythmia

cohortId <- 7362 # LEGEND cardiovascular-related mortality

cohortId <- 13567 # Test cohort with two initial event criteria

counts <- findIncludedSourceCodes(connectionDetails = connectionDetails,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  oracleTempSchema = oracleTempSchema,
                                  baseUrl = baseUrl,
                                  cohortId = cohortId,
                                  byMonth = FALSE,
                                  useSourceValues = FALSE)

counts <- findIncludedSourceCodes(connectionDetails = connectionDetails,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  oracleTempSchema = oracleTempSchema,
                                  baseUrl = baseUrl,
                                  cohortId = cohortId,
                                  byMonth = FALSE,
                                  useSourceValues = TRUE)

counts <- breakDownIndexEvents(connectionDetails = connectionDetails,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               oracleTempSchema = oracleTempSchema,
                               baseUrl = baseUrl,
                               cohortId = cohortId,
                               cohortDatabaseSchema = cohortDatabaseSchema,
                               cohortTable = cohortTable,
                               createCohortTable = TRUE,
                               instantiateCohort = TRUE,
                               instantiatedCohortId = cohortId)
