library(testthat)

if (Sys.getenv("CDM5_SQL_SERVER_SERVER") != "") {
  options(dbms = "sql server")
  test_check("CohortDiagnostics")
}