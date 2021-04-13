library(CohortDiagnostics)
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"))
tempEmulationSchema <- NULL

cdmDatabaseSchema <- "CDM_jmdc_v1063.dbo"
conceptCountsDatabaseSchema <- "scratch.dbo"
conceptCountsTable <- "concept_prevalence_counts"

# Upload concept prevalence data to database -------------------------------------

conceptCounts <- read.table("c:/temp/conceptPrevalence/cnt_all.tsv", header = TRUE, sep = "\t")
colnames(conceptCounts) <- c("concept_id", "concept_count")
conceptCounts$concept_subjects <- conceptCounts$concept_count
conceptCounts <- conceptCounts[!is.na(conceptCounts$concept_id), ]

connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::insertTable(connection = connection, 
                               tableName = paste(conceptCountsDatabaseSchema, conceptCountsTable, sep = "."),
                               data = conceptCounts,
                               dropTableIfExists = TRUE,
                               createTable = TRUE,
                               tempTable = FALSE,
                               tempEmulationSchema = tempEmulationSchema,
                               progressBar = TRUE,
                               useMppBulkLoad = FALSE)
DatabaseConnector::disconnect(connection)


# Run diagnostics -----------------------------------------------

baseUrl <- Sys.getenv("ohdsiBaseUrl")
cohortSetReferenceFile <- file.path("exampleComparativeCohortStudy", "inst", "settings", "CohortsToCreate.csv")
cohortSetReference <- read.csv(cohortSetReferenceFile)

runCohortDiagnosticsUsingExternalCounts(baseUrl = baseUrl,
                                        cohortSetReference = cohortSetReference,
                                        connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        tempEmulationSchema = tempEmulationSchema,
                                        conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                        conceptCountsTable = conceptCountsTable,
                                        exportFolder = "c:/exampleStudy/OhdsiConceptPrevalence",
                                        databaseId = "OHDSI Concept Prevalence",
                                        runIncludedSourceConcepts = TRUE,
                                        runOrphanConcepts = TRUE,
                                        minCellCount = 1) 

preMergeDiagnosticsFiles("c:/temp/exampleStudy")

launchDiagnosticsExplorer("c:/temp/exampleStudy")
