library(testthat)

if (Sys.getenv("CDM5_ORACLE_SERVER") != "") {
  options(dbms = "oracle")
  test_check("CohortDiagnostics")
}