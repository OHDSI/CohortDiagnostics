test_that("DiagnosticsExplorer loads", {
  envir <- new.env()
  initializeEnvironment(shinySettings,
                        table1SpecPath = table1SpecPath,
                        dataModelSpecificationsPath = dataModelSpecificationsPath,
                        envir = envir)

  # Environment should have initialized
  expect_true(exists("dataSource"))

  shiny::testServer(diagnosticsExplorerModule, args = list(
    id = "testAnnotationServer",
    envir = envir
  ), {
    ## input tests will go here
    session$setInputs(
      tabs = "cohortCounts",
      database = "Eunomia"
    )
    expect_null(inputCohortIds())
  })
})