library(testthat)

if (Sys.getenv("CDM5_POSTGRESQL_SERVER") != "") {
  options(dbms = "postgresql")
  test_check("CohortDiagnostics")
}