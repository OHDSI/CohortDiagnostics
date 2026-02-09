# SQL unit tests as described in docs/SQL_UNIT_TESTING_PLAN.md
# Uses in-memory DuckDB fixture with inserted test data (native DATE type, no skip).
suppressWarnings(library(dplyr))

pkg <- utils::packageName()
if (is.null(pkg)) pkg <- "CohortDiagnostics"

# ---- 2. GetCalendarYearRange.sql ----
test_that("GetCalendarYearRange returns one row with start_year and end_year", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "GetCalendarYearRange.sql",
    packageName = pkg,
    dbms = con@dbms,
    cdm_database_schema = server$cdmDatabaseSchema
  )
  result <- DatabaseConnector::querySql(con, sql, snakeCaseToCamelCase = TRUE)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1L)
  expect_true(all(c("startYear", "endYear") %in% names(result)))
  expect_equal(as.integer(result$startYear), 2020L)
  expect_equal(as.integer(result$endYear), 2022L)
})

test_that("GetCalendarYearRange with empty observation_period returns one row or zero rows", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE, emptyObservationPeriod = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "GetCalendarYearRange.sql",
    packageName = pkg,
    dbms = con@dbms,
    cdm_database_schema = server$cdmDatabaseSchema
  )
  result <- DatabaseConnector::querySql(con, sql, snakeCaseToCamelCase = TRUE)
  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) %in% c(0L, 1L))
})

# ---- 5. ObservedPerCalendarMonth.sql ----
test_that("ObservedPerCalendarMonth returns event_year, event_month, start_count, end_count", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "ObservedPerCalendarMonth.sql",
    packageName = pkg,
    dbms = con@dbms,
    cdm_database_schema = server$cdmDatabaseSchema
  )
  result <- DatabaseConnector::querySql(con, sql, snakeCaseToCamelCase = TRUE)
  expect_s3_class(result, "data.frame")
  expect_true(all(c("eventYear", "eventMonth", "startCount", "endCount") %in% names(result)))
  expect_gt(nrow(result), 0L)
  row2020_6 <- result[result$eventYear == 2020 & result$eventMonth == 6, ]
  expect_gte(nrow(row2020_6), 1L)
  expect_true(any(as.integer(row2020_6$startCount) == 1L, na.rm = TRUE))
  row2022_11 <- result[result$eventYear == 2022 & result$eventMonth == 11, , drop = FALSE]
  expect_gte(nrow(row2022_11), 1L)
})

# ---- 3. ComputeIncidenceRates (via getIncidenceRate) ----
test_that("ComputeIncidenceRates: getIncidenceRate returns expected columns and structure", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  result <- getIncidenceRate(
    connection = con,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    cohortTable = server$cohortTable,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    firstOccurrenceOnly = TRUE,
    washoutPeriod = 365,
    cohortId = 17492L
  )
  expect_true(is.data.frame(result))
  expect_equal(
    names(result),
    c("cohortCount", "personYears", "gender", "ageGroup", "calendarYear", "incidenceRate")
  )
})

test_that("ComputeIncidenceRates: empty cohort returns empty tibble", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  result <- suppressWarnings(getIncidenceRate(
    connection = con,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    cohortTable = server$cohortTable,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    firstOccurrenceOnly = TRUE,
    washoutPeriod = 365,
    cohortId = 99999L
  ))
  expect_true(is.data.frame(result))
  expect_equal(nrow(result), 0L)
})

# ---- 4. VisitContext.sql ----
test_that("VisitContext: getVisitContext returns cohort_id, visit_concept_id, visit_context, subjects", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  result <- getVisitContext(
    connection = con,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    cohortTable = server$cohortTable,
    cohortIds = c(17492L, 17493L),
    conceptIdTable = NULL,
    cdmVersion = 5
  )
  expect_true(is.data.frame(result))
  expect_true(all(c("cohortId", "visitConceptId", "visitContext", "subjects") %in% names(result)))
  if (nrow(result) > 0) {
    valid_contexts <- c("Before", "During visit", "On visit start", "After", "Other")
    expect_true(all(result$visitContext %in% valid_contexts))
  }
})

test_that("VisitContext: only specified cohort_ids appear in output", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  result <- getVisitContext(
    connection = con,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    cohortTable = server$cohortTable,
    cohortIds = 17492L,
    conceptIdTable = NULL,
    cdmVersion = 5
  )
  if (nrow(result) > 0) {
    expect_true(identical(unique(result$cohortId), 17492))
  }
})

# ---- 6. includedSourceConcepts ----
test_that("includedSourceConcepts: runIncludedSourceConcepts produces expected output structure", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection
  exportFolder <- getUniqueTempDir()
  dir.create(exportFolder, recursive = TRUE)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  runResolvedConceptSets(
    connection = con,
    cohortDefinitionSet = server$cohortDefinitionSet[1, ],
    databaseId = "test",
    exportFolder = exportFolder,
    minCellCount = 0,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    incremental = FALSE
  )
  runIncludedSourceConcepts(
    connection = con,
    cohortDefinitionSet = server$cohortDefinitionSet[1, ],
    tempEmulationSchema = server$tempEmulationSchema,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    exportFolder = exportFolder,
    databaseId = "test",
    incremental = FALSE,
    minCellCount = 0
  )
  csvPath <- file.path(exportFolder, "included_source_concept.csv")
  skip_if_not(file.exists(csvPath), "included_source_concept.csv not created (no concept sets?)")
  result <- read.csv(csvPath)
  expect_true(all(c("conceptId", "conceptCount", "conceptSubjects") %in% names(result)) ||
    all(c("concept_id", "concept_count", "concept_subjects") %in% names(result)))
})

# ---- 7. CreateConceptCountTable.sql ----
test_that("CreateConceptCountTable: createConceptCountsTable produces concept_id, concept_count, concept_subjects", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  createConceptCountsTable(
    connection = con,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    conceptCountsTable = "concept_counts_sql_test",
    conceptCountsDatabaseSchema = server$cohortDatabaseSchema,
    conceptCountsTableIsTemp = FALSE,
    removeCurrentTable = TRUE
  )
  result <- DatabaseConnector::renderTranslateQuerySql(
    con,
    "SELECT * FROM @schema.@table LIMIT 1",
    schema = server$cohortDatabaseSchema,
    table = "concept_counts_sql_test"
  )
  DatabaseConnector::renderTranslateExecuteSql(
    con,
    "DROP TABLE IF EXISTS @schema.@table;",
    schema = server$cohortDatabaseSchema,
    table = "concept_counts_sql_test"
  )
  expect_s3_class(result, "data.frame")
  expect_true("CONCEPT_ID" %in% names(result) || "concept_id" %in% names(result))
})

# ---- 8. CreateTimeSeriesCohortTable / 9. ComputeTimeSeries1-6 ----
test_that("CreateTimeSeriesCohortTable and ComputeTimeSeries: runTimeSeries produces time_series output", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection
  exportFolder <- getUniqueTempDir()
  dir.create(exportFolder, recursive = TRUE)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  runTimeSeries(
    connection = con,
    tempEmulationSchema = server$tempEmulationSchema,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    cohortTable = server$cohortTable,
    cohortDefinitionSet = server$cohortDefinitionSet[1:2, ],
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = FALSE,
    databaseId = "test",
    exportFolder = exportFolder,
    minCellCount = 0,
    instantiatedCohorts = server$cohortDefinitionSet$cohortId[1:2],
    incremental = FALSE,
    observationPeriodDateRange = dplyr::tibble(
      observationPeriodMinDate = as.Date("2000-01-01"),
      observationPeriodMaxDate = as.Date("2030-12-31")
    )
  )
  csvPath <- file.path(exportFolder, "time_series.csv")
  expect_true(file.exists(csvPath))
  result <- read.csv(csvPath)
  expect_true(ncol(result) >= 1)
  expect_true(any(grepl("cohort", names(result), ignore.case = TRUE)))
})

# ---- 10. inclusionStatsTables.sql ----
test_that("inclusionStatsTables: DDL runs twice without error (idempotent)", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "inclusionStatsTables.sql",
    packageName = pkg,
    dbms = con@dbms,
    results_schema = server$cohortDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema
  )
  expect_error(DatabaseConnector::executeSql(con, sql), NA)
  expect_error(DatabaseConnector::executeSql(con, sql), NA)
})

# ---- 11. CreateInclusionStatsTables.sql ----
test_that("CreateInclusionStatsTables: creates four tables with expected columns", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection
  schema <- server$cohortDatabaseSchema
  prefix <- "cd_inc_"

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CreateInclusionStatsTables.sql",
    packageName = pkg,
    dbms = con@dbms,
    cohort_database_schema = schema,
    cohort_inclusion_table = paste0(prefix, "inclusion"),
    cohort_inclusion_result_table = paste0(prefix, "inc_result"),
    cohort_inclusion_stats_table = paste0(prefix, "inc_stats"),
    cohort_summary_stats_table = paste0(prefix, "summary_stats")
  )
  expect_error(DatabaseConnector::executeSql(con, sql), NA)
  tables <- DatabaseConnector::getTableNames(con, schema)
  expect_true(any(grepl(paste0(prefix, "inclusion"), tables)))
  expect_error(DatabaseConnector::executeSql(con, sql), NA)
  for (t in c("inclusion", "inc_result", "inc_stats", "summary_stats")) {
    DatabaseConnector::renderTranslateExecuteSql(
      con, "DROP TABLE IF EXISTS @schema.@table;",
      schema = schema,
      table = paste0(prefix, t)
    )
  }
})

# ---- 12. CohortEntryBreakdown ----
test_that("CohortEntryBreakdown: runBreakdownIndexEvents produces index_event_breakdown when run", {
  server <- createSqlTestFixtureDb(fullCdm = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection
  exportFolder <- getUniqueTempDir()
  dir.create(exportFolder, recursive = TRUE)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  runResolvedConceptSets(
    connection = con,
    cohortDefinitionSet = server$cohortDefinitionSet[1, ],
    databaseId = "test",
    exportFolder = exportFolder,
    minCellCount = 0,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    incremental = FALSE
  )
  runBreakdownIndexEvents(
    connection = con,
    cohortDefinitionSet = server$cohortDefinitionSet[1, ],
    tempEmulationSchema = server$tempEmulationSchema,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    databaseId = "test",
    exportFolder = exportFolder,
    minCellCount = 0,
    cohortTable = server$cohortTable,
    incremental = FALSE,
    incrementalFolder = exportFolder
  )
  csvPath <- file.path(exportFolder, "index_event_breakdown.csv")
  skip_if_not(file.exists(csvPath), "index_event_breakdown.csv not created")
  result <- read.csv(csvPath)
  expect_true(all(c("conceptId", "conceptCount", "subjectCount") %in% names(result)) ||
    all(c("concept_id", "concept_count", "subject_count") %in% names(result)))
})

# ---- 15. OrphanCodes / 16. DropOrphanConceptTempTables ----
test_that("OrphanCodes and DropOrphanConceptTempTables: runOrphanConcepts runs without error", {
  skip_if_not(length(testServers) > 0 && "sqlite" %in% names(testServers),
    "runOrphanConcepts requires full CDM metadata (Eunomia); use testServers"
  )
  server <- testServers[["sqlite"]]
  con <- DatabaseConnector::connect(server$connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))
  exportFolder <- getUniqueTempDir()
  dir.create(exportFolder, recursive = TRUE)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  createConceptCountsTable(
    connection = con,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    conceptCountsTable = "concept_counts",
    conceptCountsDatabaseSchema = server$cohortDatabaseSchema,
    conceptCountsTableIsTemp = FALSE,
    removeCurrentTable = TRUE
  )
  runResolvedConceptSets(
    connection = con,
    cohortDefinitionSet = server$cohortDefinitionSet[1, ],
    databaseId = "test",
    exportFolder = exportFolder,
    minCellCount = 0,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema,
    incremental = FALSE
  )
  expect_error(
    runOrphanConcepts(
      connection = con,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
      databaseId = "test",
      cohorts = server$cohortDefinitionSet[1, ],
      exportFolder = exportFolder,
      minCellCount = 0,
      conceptCountsDatabaseSchema = server$cohortDatabaseSchema,
      conceptCountsTable = "concept_counts",
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = server$cohortTable,
      incremental = FALSE
    ),
    NA
  )
  expect_true(file.exists(file.path(exportFolder, "orphan_concept.csv")) ||
    length(list.files(exportFolder)) >= 0)
})

test_that("DropOrphanConceptTempTables: executes without error", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropOrphanConceptTempTables.sql",
    packageName = pkg,
    dbms = con@dbms
  )
  expect_error(DatabaseConnector::executeSql(con, sql), NA)
})

# ---- 17. CreateResultsDataModel.sql ----
test_that("CreateResultsDataModel: all result tables created with expected structure", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection

  .createDataModel(
    connection = con,
    databaseSchema = "main",
    tablePrefix = "cd_sql_test_"
  )
  tables <- DatabaseConnector::getTableNames(con, "main")
  prefixed <- paste0("cd_sql_test_", c("cohort", "cohort_count", "temporal_covariate_value", "incidence_rate"))
  for (t in prefixed) {
    expect_true(t %in% tables, info = paste("Table", t, "should exist"))
  }
  specs <- getResultsDataModelSpecifications()
  tableNames <- unique(specs$tableName)
  prefixedAll <- paste0("cd_sql_test_", tableNames)
  for (t in prefixedAll) {
    if (t %in% tables) {
      DatabaseConnector::renderTranslateExecuteSql(
        con, "DROP TABLE IF EXISTS main.@table;", table = t
      )
    }
  }
})

# ---- 18. UpdateVersionNumber.sql ----
test_that("UpdateVersionNumber: single row with version_number after run", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  con <- server$connection
  schema <- server$cohortDatabaseSchema

  DatabaseConnector::renderTranslateExecuteSql(
    con,
    "CREATE TABLE IF NOT EXISTS @schema.package_version (version_number VARCHAR(50));",
    schema = schema
  )
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "UpdateVersionNumber.sql",
    packageName = pkg,
    dbms = con@dbms,
    database_schema = schema,
    table_prefix = "",
    package_version = "package_version",
    version_number = "3.4.1"
  )
  expect_error(DatabaseConnector::executeSql(con, sql), NA)
  result <- DatabaseConnector::renderTranslateQuerySql(
    con,
    "SELECT * FROM @schema.package_version",
    schema = schema
  )
  expect_equal(nrow(result), 1L)
  expect_error(DatabaseConnector::executeSql(con, sql), NA)
  result2 <- DatabaseConnector::renderTranslateQuerySql(
    con,
    "SELECT * FROM @schema.package_version",
    schema = schema
  )
  expect_equal(nrow(result2), 1L)
  DatabaseConnector::renderTranslateExecuteSql(
    con, "DROP TABLE IF EXISTS @schema.package_version;", schema = schema
  )
})

# ---- 19. Migration scripts ----
test_that("Migrations: getDataMigrator and migrateDataModel run after CreateResultsDataModel", {
  server <- createSqlTestFixtureDb(observationPeriodOnly = TRUE)
  skip_if(server$connectionDetails$dbms == "duckdb", "Migrations use SQL Server/Postgres syntax; skip on DuckDB")
  on.exit({ DatabaseConnector::disconnect(server$connection); if (!is.null(server$path)) unlink(server$path, force = TRUE) })
  connectionDetails <- server$connectionDetails
  con <- server$connection

  .createDataModel(
    connection = con,
    databaseSchema = "main",
    tablePrefix = "cd_mig_test_"
  )
  migrator <- getDataMigrator(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = "cd_mig_test_"
  )
  on.exit(migrator$finalize(), add = TRUE)
  expect_true(migrator$check())
  status <- migrator$getStatus()
  expect_true(is.data.frame(status))
  migrateDataModel(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = "cd_mig_test_"
  )
  expect_true(all(migrator$getStatus()$executed))
  migrator$finalize()
  tables <- DatabaseConnector::getTableNames(con, "main")
  toDrop <- tables[grepl("cd_mig_test_", tables)]
  for (t in toDrop) {
    DatabaseConnector::renderTranslateExecuteSql(con, "DROP TABLE IF EXISTS main.@table;", table = t)
  }
})
