library(StudyDiagnostics)

connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"),
                                             schema = "CDM_Truven_MDCR_V415")
oracleTempSchema <- NULL
workDatabaseSchema <- "scratch.dbo"
baseUrl <- Sys.getenv("baseUrl")

# Using private cohort table:
cdmDatabaseSchema <- "CDM_IBM_MDCR_V1062.dbo"
cohortDatabaseSchema <- workDatabaseSchema
resultDatabaseSchema <- workDatabaseSchema
cohortTable <- "mschuemi_temp"

# Using ATLAS cohort table:
cohortDatabaseSchema <- "CDM_IBM_MDCR_V1062.dbo"
resultDatabaseSchema <- "CDM_IBM_MDCR_V1062.ohdsi_results"

cohortId <- 7399 # LEGEND Cardiac Arrhythmia

cohortId <- 7362 # LEGEND cardiovascular-related mortality

cohortId <- 13567 # Test cohort with two initial event criteria

cohortId <- 5665 # Zoledronic acid new users with prostate cancer (many inclusion rules)


# Cohort construction (when not using ATLAS cohorts) -------------------------------------
createCohortTable(connectionDetails = connectionDetails,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  createInclusionStatsTables = TRUE)

instantiateCohort(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  oracleTempSchema = oracleTempSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  baseUrl = baseUrl,
                  cohortId = cohortId,
                  generateInclusionStats = TRUE)

# Source concepts -------------------------------------------------------------------------
createConceptCountsTable(connectionDetails = connectionDetails,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         workDatabaseSchema = workDatabaseSchema)

includedSourceConcepts <- findCohortIncludedSourceConcepts(connectionDetails = connectionDetails,
                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                           oracleTempSchema = oracleTempSchema,
                                                           baseUrl = baseUrl,
                                                           cohortId = cohortId,
                                                           byMonth = FALSE,
                                                           useSourceValues = FALSE)

orphanConcepts <- findCohortOrphanConcepts(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           oracleTempSchema = oracleTempSchema,
                                           workDatabaseSchema = workDatabaseSchema,
                                           baseUrl = baseUrl,
                                           cohortId = cohortId)

# Cohort-level ------------------------------------------------------------------
breakdown <- breakDownIndexEvents(connectionDetails = connectionDetails,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  oracleTempSchema = oracleTempSchema,
                                  baseUrl = baseUrl,
                                  cohortId = cohortId,
                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                  cohortTable = cohortTable,
                                  instantiatedCohortId = cohortId)

incidenceProportion <- getIncidenceProportion(connectionDetails = connectionDetails,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              cohortTable = cohortTable,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              firstOccurrenceOnly = TRUE,
                                              minObservationTime = 365,
                                              instantiatedCohortId = cohortId)

plotIncidenceProportionByYear(incidenceProportion)

plotIncidenceProportion(incidenceProportion, restrictToFullAgeData = TRUE)
