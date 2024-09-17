library(testthat)

if (Sys.getenv("SKIP_DB_TESTS") != "TRUE") {
  options(dbms = "postgresql")
  test_check("CohortDiagnostics")
}
