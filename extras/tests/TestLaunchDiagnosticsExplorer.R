# Test that Diagnostics Explorer starts without error.
# Run from package root: Rscript extras/tests/TestLaunchDiagnosticsExplorer.R
# Or with timeout (app blocks): timeout 20 Rscript extras/tests/TestLaunchDiagnosticsExplorer.R

options(warn = 1)

if (!requireNamespace("CohortDiagnostics", quietly = TRUE)) {
  devtools::load_all()
}
library(CohortDiagnostics)

# Create minimal SQLite with schema + one database, one cohort, one cohort_count
# so the app can start without needing full Eunomia run.
sqlitePath <- tempfile(fileext = ".sqlite")
on.exit(unlink(sqlitePath, force = TRUE), add = TRUE)

message("Creating minimal results SQLite...")
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqlitePath)
connection <- DatabaseConnector::connect(connectionDetails)
on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

CohortDiagnostics::createResultsDataModel(
  connectionDetails = connectionDetails,
  databaseSchema = "main",
  tablePrefix = ""
)

# Insert minimal data so createCdDatabaseDataSource and UI don't error
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
on.exit(unlink(sqlitePath, force = TRUE), add = TRUE)

message("Launching Diagnostics Explorer (will block until app is closed or process is killed)...")
CohortDiagnostics::launchDiagnosticsExplorer(
  sqliteDbPath = sqlitePath,
  launch.browser = FALSE
)

message("App exited normally.")
