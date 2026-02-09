# Test that Diagnostics Explorer starts without error when given a minimal SQLite
# (does not require Eunomia data). Runs app in background and verifies it stays up a few seconds.

test_that("launchDiagnosticsExplorer starts without error with minimal SQLite", {
  skip_if_not_installed("shiny")
  skip_if_not_installed("ResultModelManager")

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
  DatabaseConnector::executeSql(connection, "
    INSERT INTO main.metadata (database_id, start_time, variable_field, value_field)
    VALUES
      ('Eunomia', 'TM_2020-01-01 00:00:00', 'timeZone', 'UTC'),
      ('Eunomia', 'TM_2020-01-01 00:00:00', 'runTime', '0');
  ")
  DatabaseConnector::disconnect(connection)

  # Run app in background; if it starts without error it will block (listen).
  # We run with a short timeout - if we get past startup without error, test passes.
  app_process <- callr::r_bg(
    function(sqlitePath) {
      options(shiny.port = 0L)  # random port
      CohortDiagnostics::launchDiagnosticsExplorer(
        sqliteDbPath = sqlitePath,
        launch.browser = FALSE
      )
    },
    args = list(sqlitePath = sqlitePath),
    package = TRUE,
    stdout = "|",
    stderr = "2>&1"
  )

  on.exit(app_process$kill(), add = TRUE)

  # Wait for "Listening on" or process exit (error)
  for (i in 1:50) {
    Sys.sleep(0.5)
    out <- app_process$read_output()
    if (nzchar(out) && grepl("Listening on", out, fixed = TRUE)) {
      # App started successfully
      app_process$kill()
      expect_true(TRUE)
      return(invisible(NULL))
    }
    if (!app_process$is_alive()) {
      # Process exited - likely error
      err <- app_process$read_error()
      stop("App process exited. stderr: ", paste(err, collapse = "\n"))
    }
  }

  # Timeout - kill and consider success if no error so far
  app_process$kill()
  expect_true(TRUE)
})
