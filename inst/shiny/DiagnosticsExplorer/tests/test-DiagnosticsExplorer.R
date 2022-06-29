test_that("DiagnosticsExplorer loads", {
  initializeEnvironment(shinySettings,
                        table1SpecPath = table1SpecPath,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)

  # Environment should have initialized
  expect_true(exists("dataSource"))

  shiny::testServer(diagnosticsExplorerModule, args = list(
    id = "testAnnotationServer",
    dataSource = dataSource,
    databaseTable = database,
    cohortTable = cohort,
    enableAnnotation = enableAnnotation,
    enableAuthorization = enableAuthorization,
    enabledTabs = enabledTabs,
    conceptSets = conceptSets,
    userCredentials = userCredentials,
    activeUser = activeUser
  ), {
    ## input tests will go here
    session$setInputs(
      tabs = "cohortCounts",
      database = "Eunomia"
    )
    expect_null(inputCohortIds())

  })
})