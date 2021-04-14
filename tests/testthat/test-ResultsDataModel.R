jdbcDriverFolder <- tempfile("jdbcDrivers")
DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                                                                password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
                                                                server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                                                                pathToDriver = jdbcDriverFolder)

cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
cohortDiagnosticsSchema <- Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")
oracleTempSchema <- NULL
cohortTable <- "cohort"
connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
folder <- tempfile("cohortDiagnosticsTest")

withr::defer({
  DatabaseConnector::disconnect(connection)
  unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
  unlink(folder, recursive = TRUE, force = TRUE)
}, testthat::teardown_env())


#' Only works with postgres > 9.4
.tableExists <- function(connection, schema, tableName) {
  return(!is.na(DatabaseConnector::renderTranslateQuerySql(connection, "SELECT to_regclass('@schema.@table');",
                                                           table = tableName,
                                                           schema = schema))[[1]])
}


test_that("Create schema", {
  createResultsDataModel(connectionDetails = connectionDetails, schema = cohortDiagnosticsSchema)
  assert_true(.tableExists(connection, "analysis_ref"))
  assert_true(.tableExists(connection, "cohort"))
  assert_true(.tableExists(connection, "cohort_count"))
  assert_true(.tableExists(connection, "cohort_overlap"))
  assert_true(.tableExists(connection, "concept"))
  assert_true(.tableExists(connection, "concept_ancestor"))
  assert_true(.tableExists(connection, "concept_relationship"))
  assert_true(.tableExists(connection, "concept_sets"))
  assert_true(.tableExists(connection, "concept_synonym"))
  assert_true(.tableExists(connection, "covariate_ref"))
  assert_true(.tableExists(connection, "covariate_value"))
  assert_true(.tableExists(connection, "database"))
  assert_true(.tableExists(connection, "domain"))
  assert_true(.tableExists(connection, "incidence_rate"))
  assert_true(.tableExists(connection, "included_source_concept"))
  assert_true(.tableExists(connection, "inclusion_rule_stats"))
  assert_true(.tableExists(connection, "index_event_breakdown"))
  assert_true(.tableExists(connection, "metadata"))
  assert_true(.tableExists(connection, "orphan_concept"))
  assert_true(.tableExists(connection, "phenotype_description"))
  assert_true(.tableExists(connection, "relationship"))
  assert_true(.tableExists(connection, "temporal_analysis_ref"))
  assert_true(.tableExists(connection, "temporal_covariate_ref"))
  assert_true(.tableExists(connection, "temporal_covariate_value"))
  assert_true(.tableExists(connection, "temporal_time_ref"))
  assert_true(.tableExists(connection, "time_distribution"))
  assert_true(.tableExists(connection, "visit_context"))
  assert_true(.tableExists(connection, "vocabulary"))

  # Bad schema name
  expect_error(createResultsDataModel(connection = connection, schema = "non_existant_schema"))
})


test_that("Results upload", {
  instantiateCohortSet(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDiagnosticsSchema,
                       cohortTable = cohortTable,
                       cohortIds = c(17492, 17692),
                       packageName = "CohortDiagnostics",
                       cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
                       generateInclusionStats = TRUE,
                       createCohortTable = TRUE,
                       inclusionStatisticsFolder = file.path(folder, "incStats"))

  runCohortDiagnostics(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDiagnosticsSchema,
                       cohortTable = cohortTable,
                       cohortIds = c(17492, 17692),
                       packageName = "CohortDiagnostics",
                       cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
                       inclusionStatisticsFolder = file.path(folder, "incStats"),
                       exportFolder = file.path(folder, "export"),
                       databaseId = "cdmv5",
                       runInclusionStatistics = TRUE,
                       runBreakdownIndexEvents = TRUE,
                       runCohortCharacterization = TRUE,
                       runTemporalCohortCharacterization = TRUE,
                       runCohortOverlap = TRUE,
                       runIncidenceRate = TRUE,
                       runIncludedSourceConcepts = TRUE,
                       runOrphanConcepts = TRUE,
                       runTimeDistributions = TRUE,
                       incremental = TRUE,
                       incrementalFolder = file.path(folder, "incremental"))

  listOfZipFilesToUpload <- list.files(path = file.path(folder, "export"),
                                       pattern = ".zip",
                                       full.names = TRUE,
                                       recursive = TRUE)

  for (i in (1:length(listOfZipFilesToUpload))) {
    uploadResults(connectionDetails = connectionDetails,
                  schema = cohortDiagnosticsSchema,
                  zipFileName = listOfZipFilesToUpload[[i]])
  }
})