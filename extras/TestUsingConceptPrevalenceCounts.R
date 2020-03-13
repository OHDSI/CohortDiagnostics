library(CohortDiagnostics)
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"))
oracleTempSchema <- NULL

cdmDatabaseSchema <- "CDM_jmdc_v1063.dbo"
conceptCountsDatabaseSchema <- "scratch.dbo"
conceptCountsTable <- "concept_prevalence_counts"

# Upload concept prevalence data to database -------------------------------------

conceptCounts <- read.table("c:/temp/conceptPrevalence/counts_for_22db.tsv", header = TRUE, sep = "\t")
colnames(conceptCounts) <- c("concept_count", "concept_id")
conceptCounts$concept_subjects <- conceptCounts$concept_count

connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::insertTable(connection = connection, 
                               tableName = paste(conceptCountsDatabaseSchema, conceptCountsTable, sep = "."),
                               data = conceptCounts,
                               dropTableIfExists = TRUE,
                               createTable = TRUE,
                               tempTable = FALSE,
                               oracleTempSchema = oracleTempSchema,
                               progressBar = TRUE)
DatabaseConnector::disconnect(connection)


# Run diagnostics -----------------------------------------------

baseUrl <- Sys.getenv("ohdsiBaseUrl")
cohortSetReferenceFile <- file.path("exampleComparativeCohortStudy", "inst", "settings", "CohortsToCreate.csv")
cohortSetReference <- read.csv(cohortSetReferenceFile)

runCohortDiagnosticsUsingExternalCounts(baseUrl = baseUrl,
                                        cohortSetReference = cohortSetReference,
                                        connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        oracleTempSchema = oracleTempSchema,
                                        conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                        conceptCountsTable = conceptCountsTable,
                                        exportFolder = "c:/exampleStudy/OhdsiConceptPrevalence",
                                        databaseId = "OHDSI Concept Prevalence",
                                        runIncludedSourceConcepts = TRUE,
                                        runOrphanConcepts = FALSE,
                                        minCellCount = 1) 

preMergeDiagnosticsFiles("c:/temp/exampleStudy")

launchDiagnosticsExplorer("c:/temp/exampleStudy")
