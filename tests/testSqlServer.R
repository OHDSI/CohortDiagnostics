library(testthat)

if (Sys.getenv("SKIP_DB_TESTS") != "TRUE") {
  options(dbms = "sql server")
  test_check("CohortDiagnostics")
}
