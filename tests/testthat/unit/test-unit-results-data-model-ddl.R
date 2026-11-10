# Unit tests for the results data model DDL.
#
# These run against a real PostgreSQL server when one is available, because the
# failure this guards against (declaring a column in both
# CreateResultsDataModel.sql and a migration) cannot be detected by inspecting
# SQL strings: it only appears when createResultsDataModel() creates the tables
# and then runs the migrations on an actual database.
#
# In CI the postgres service is started by .github/workflows/unit-tests.yaml and
# discovered through standard POSTGRES_* environment variables. When no server is
# reachable the tests skip, so a plain local `test_local()` still works.

# Resolve a file shipped in inst/. Works when the package is installed (the CI
# path, via test_local(load_package = "installed")) and also when it has been
# loaded with pkgload::load_all(), where system.file() does not see inst/.
packageFile <- function(...) {
  path <- system.file(..., package = "CohortDiagnostics")
  if (nzchar(path) && file.exists(path)) {
    return(path)
  }
  srcRoot <- tryCatch(pkgload::pkg_path(), error = function(e) NULL)
  if (!is.null(srcRoot) && nzchar(srcRoot)) {
    candidate <- file.path(srcRoot, "inst", ...)
    if (file.exists(candidate)) {
      return(candidate)
    }
  }
  return(path)
}

# Column names declared for index_event_breakdown in CreateResultsDataModel.sql.
# Used to assert that cohort_start_year is NOT declared there - it must come
# from the migration only.
.indexEventBreakdownDdlColumns <- function() {
  ddlPath <- packageFile("sql", "sql_server", "CreateResultsDataModel.sql")
  stopifnot(file.exists(ddlPath))
  ddl <- SqlRender::readSql(ddlPath)

  lines <- strsplit(ddl, "\n", fixed = TRUE)[[1]]
  startIdx <- grep(
    "CREATE TABLE @results_schema.@table_prefix@index_event_breakdown",
    lines,
    fixed = TRUE
  )
  stopifnot(length(startIdx) == 1)
  endRel <- grep(";", lines[startIdx:length(lines)], fixed = TRUE)[1]
  block <- lines[startIdx:(startIdx + endRel - 1)]

  # column lines look like "\t\t\tconcept_id BIGINT NOT NULL,"
  columnLines <- grep("^\\s+[a-z_]+ +(BIGINT|INT|VARCHAR|FLOAT|TEXT)", block, value = TRUE)
  columnNames <- sub("^\\s+([a-z_]+).*$", "\\1", columnLines)
  return(unique(columnNames))
}

# TRUE when a postgres server and JDBC driver have been configured. Cleanly
# skipped everywhere else (including a bare local `test_local()`).
canTestResultsDataModelDdl <- function() {
  if (!requireNamespace("DatabaseConnector", quietly = TRUE)) {
    return(FALSE)
  }

  jars <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER", unset = "")
  if (!nzchar(jars) || !dir.exists(jars)) {
    return(FALSE)
  }
  # only opt in when a server is explicitly configured
  return(nzchar(Sys.getenv("CD_TEST_POSTGRES_HOST", unset = "")) ||
    nzchar(Sys.getenv("POSTGRES_PASSWORD", unset = "")))
}

resultsDataModelTestConnectionDetails <- function() {
  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("CD_TEST_POSTGRES_HOST", unset = "localhost/postgres"),
    port = as.integer(Sys.getenv("CD_TEST_POSTGRES_PORT", unset = "5432")),
    user = Sys.getenv("CD_TEST_POSTGRES_USER", unset = "postgres"),
    password = Sys.getenv("POSTGRES_PASSWORD", unset = "postgres"),
    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  )
}

# Resolve a file shipped in inst/ whether the package is installed or loaded
# with pkgload::load_all() (which is what test_local() does - under pkgload,
# system.file() does not see inst/).
packageFile <- function(...) {
  path <- system.file(..., package = "CohortDiagnostics")
  if (nzchar(path) && file.exists(path)) {
    return(path)
  }
  # pkgload keeps the source tree on the search path via its own package name
  srcRoot <- tryCatch(
    {
      ns <- getNamespace("CohortDiagnostics")
      pkgload::pkg_path()
    },
    error = function(e) NULL
  )
  if (!is.null(srcRoot)) {
    candidate <- file.path(srcRoot, "inst", ...)
    if (file.exists(candidate)) {
      return(candidate)
    }
  }
  return(path)
}

testSchema <- "cd_ddl_test"

resetScratchSchema <- function(connection) {
  DatabaseConnector::executeSql(
    connection,
    sprintf("DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s;", testSchema, testSchema),
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}

dropScratchSchema <- function(connection) {
  try(
    DatabaseConnector::executeSql(
      connection,
      sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", testSchema),
      progressBar = FALSE,
      reportOverallTime = FALSE
    ),
    silent = TRUE
  )
}

test_that("createResultsDataModel creates every table and records the migrations", {
  skip_if_not(canTestResultsDataModelDdl(), "No PostgreSQL server or JDBC driver configured")

  connectionDetails <- resultsDataModelTestConnectionDetails()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit({
    dropScratchSchema(connection)
    DatabaseConnector::disconnect(connection)
  }, add = TRUE)
  resetScratchSchema(connection)

  # The exact call that failed when cohort_start_year was declared in both the
  # DDL and Migration_8: the DDL created the column, then the migration tried to
  # add it again and raised "column ... already exists".
  expect_no_error(
    CohortDiagnostics::createResultsDataModel(
      connectionDetails = connectionDetails,
      databaseSchema = testSchema
    )
  )

  tables <- DatabaseConnector::querySql(
    connection,
    sprintf(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s';",
      testSchema
    )
  )[[1]]

  specificationTables <- unique(CohortDiagnostics::getResultsDataModelSpecifications()$tableName)
  missing <- setdiff(specificationTables, tables)
  expect_equal(missing, character(0))

  # migrations must be recorded, otherwise they would re-run on every call
  migrations <- DatabaseConnector::querySql(
    connection,
    sprintf("SELECT migration_file FROM %s.migration;", testSchema)
  )[[1]]
  expect_true("Migration_8-v4_0_0_index_event_breakdown_cohort_start_year.sql" %in% migrations)
})

test_that("cohort_start_year is added by the migration, not the DDL", {
  skip_if_not(canTestResultsDataModelDdl(), "No PostgreSQL server or JDBC driver configured")

  connectionDetails <- resultsDataModelTestConnectionDetails()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit({
    dropScratchSchema(connection)
    DatabaseConnector::disconnect(connection)
  }, add = TRUE)
  resetScratchSchema(connection)

  CohortDiagnostics::createResultsDataModel(
    connectionDetails = connectionDetails,
    databaseSchema = testSchema
  )

  columns <- DatabaseConnector::querySql(
    connection,
    sprintf(
      "SELECT column_name FROM information_schema.columns
        WHERE table_schema = '%s' AND table_name = 'index_event_breakdown'
        ORDER BY ordinal_position;",
      testSchema
    )
  )[[1]]

  # present in the migrated database...
  expect_true("cohort_start_year" %in% columns)

  # ...but absent from the DDL, which is what caused the "already exists" error
  # when it was declared in both places
  expect_false("cohort_start_year" %in% .indexEventBreakdownDdlColumns())
})

test_that("migrating a legacy index_event_breakdown preserves its rows", {
  skip_if_not(canTestResultsDataModelDdl(), "No PostgreSQL server or JDBC driver configured")

  connectionDetails <- resultsDataModelTestConnectionDetails()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit({
    dropScratchSchema(connection)
    DatabaseConnector::disconnect(connection)
  }, add = TRUE)
  resetScratchSchema(connection)

  # Build the pre-change schema from the DDL, which has no cohort_start_year,
  # then add a row of the shape an earlier release would have written.
  DatabaseConnector::executeSql(
    connection,
    SqlRender::loadRenderTranslateSql(
      sqlFilename = "CreateResultsDataModel.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      results_schema = testSchema,
      table_prefix = ""
    ),
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  preMigrationColumns <- DatabaseConnector::querySql(
    connection,
    sprintf(
      "SELECT column_name FROM information_schema.columns
        WHERE table_schema = '%s' AND table_name = 'index_event_breakdown';",
      testSchema
    )
  )[[1]]
  expect_false("cohort_start_year" %in% preMigrationColumns)

  DatabaseConnector::executeSql(
    connection,
    sprintf(
      "INSERT INTO %s.index_event_breakdown
         (concept_id, concept_count, subject_count, cohort_id, database_id, domain_field, domain_table)
       VALUES (201826, 100, 90, 1, 'legacy', 'condition_concept_id', 'condition_occurrence');",
      testSchema
    ),
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  # Applying the migrations must not fail and must not lose the row.
  expect_no_error(
    CohortDiagnostics::migrateDataModel(
      connectionDetails = connectionDetails,
      databaseSchema = testSchema
    )
  )

  rows <- DatabaseConnector::querySql(
    connection,
    sprintf(
      "SELECT concept_id, concept_count, cohort_start_year FROM %s.index_event_breakdown;",
      testSchema
    )
  )

  expect_equal(nrow(rows), 1)
  # read by position: DatabaseConnector versions differ in whether they return
  # column names in upper or lower case
  expect_equal(rows[[1]], 201826)
  expect_equal(rows[[2]], 100)
  # backfilled by Migration_8 as "year not recorded"
  expect_equal(rows[[3]], -1)
})
