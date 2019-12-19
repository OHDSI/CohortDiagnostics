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
