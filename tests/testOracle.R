library(testthat)

if (Sys.getenv("SKIP_DB_TESTS") != "TRUE") {
  options(dbms = "oracle")
  test_check("CohortDiagnostics")
}