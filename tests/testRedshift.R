library(testthat)

if (Sys.getenv("SKIP_DB_TESTS") != "TRUE") {
  options(dbms = "redshift")
  test_check("CohortDiagnostics")
}
