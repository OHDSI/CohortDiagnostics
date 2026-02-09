# shinytest2 tests for Diagnostics Explorer
#
# Requires: install.packages("shinytest2")
# Run locally (shinytest2 skips on CRAN): NOT_CRAN=true testthat::test_file("tests/testthat/test-shinytest2-DiagnosticsExplorer.R")
# Or: testthat::test_dir("tests/testthat", filter = "shinytest2")
#
# First run creates snapshots in tests/testthat/_snaps/shinytest2-DiagnosticsExplorer/.
# Commit those files; later runs compare against them. To update snapshots: testthat::snapshot_accept("shinytest2-DiagnosticsExplorer").

test_that("Diagnostics Explorer loads with minimal SQLite (shinytest2)", {
  skip_if_not_installed("shinytest2")
  skip_if_not_installed("ResultModelManager")

  # Create minimal SQLite so the app has valid data
  sqlitePath <- tempfile(fileext = ".sqlite")
  on.exit(unlink(sqlitePath, force = TRUE))

  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqlitePath)
  connection <- DatabaseConnector::connect(connectionDetails)

  CohortDiagnostics::createResultsDataModel(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = ""
  )

  DatabaseConnector::executeSql(connection, "
    INSERT INTO main.database (database_id, database_name, description, is_meta_analysis)
    VALUES ('Eunomia', 'Eunomia Test', 'Test database', '0');
  ")
  DatabaseConnector::executeSql(connection, "
    INSERT INTO main.cohort (cohort_id, cohort_name, sql, json)
    VALUES (1, 'Test cohort', 'SELECT 1', '{}');
  ")
  DatabaseConnector::executeSql(connection, "
    INSERT INTO main.cohort_count (cohort_id, cohort_entries, cohort_subjects, database_id)
    VALUES (1, 100, 50, 'Eunomia');
  ")
  # Minimal metadata so database information module does not error (expects variableField/valueField)
  DatabaseConnector::executeSql(connection, "
    INSERT INTO main.metadata (database_id, start_time, variable_field, value_field)
    VALUES
      ('Eunomia', 'TM_2020-01-01 00:00:00', 'timeZone', 'UTC'),
      ('Eunomia', 'TM_2020-01-01 00:00:00', 'runTime', '0');
  ")
  DatabaseConnector::disconnect(connection)

  # Copy app to temp dir and point config at our sqlite (config uses relative path data/Merged...)
  appDir <- system.file("shiny", "DiagnosticsExplorer", package = "CohortDiagnostics")
  testAppDir <- tempfile("DiagnosticsExplorer_test")
  on.exit(unlink(testAppDir, recursive = TRUE, force = TRUE), add = TRUE)

  dir.create(testAppDir, recursive = TRUE)
  file.copy(list.files(appDir, full.names = TRUE, no.. = TRUE), testAppDir, recursive = TRUE)
  dir.create(file.path(testAppDir, "data"), showWarnings = FALSE)
  file.copy(sqlitePath, file.path(testAppDir, "data", "MergedCohortDiagnosticsData.sqlite"))
  # Copy ref and www dirs so app can find them when run from copy (system.file returns "" in subprocess)
  refDir <- system.file("cohort-diagnostics-ref", package = "CohortDiagnostics")
  if (dir.exists(refDir)) {
    file.copy(refDir, testAppDir, recursive = TRUE)
  }
  wwwDir <- system.file("cohort-diagnostics-www", package = "CohortDiagnostics")
  if (dir.exists(wwwDir)) {
    file.copy(wwwDir, testAppDir, recursive = TRUE)
  }

  # Launch app with shinytest2 and take a snapshot of initial state
  app <- shinytest2::AppDriver$new(
    name = "DiagnosticsExplorer",
    app_dir = testAppDir,
    load_timeout = 20000,
    seed = 42
  )
  on.exit(app$stop(), add = TRUE)

  # Expect the app to have loaded (snapshot inputs/outputs; first run creates snapshot)
  app$expect_values()
})
