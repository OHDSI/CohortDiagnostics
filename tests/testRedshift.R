library(testthat)

if (Sys.getenv("CDM5_REDSHIFT_SERVER") != "") {
  options(dbms = "redshift")
  test_check("CohortDiagnostics")
}
