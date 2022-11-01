test_that("Incidence Rate Page", {
  initializeEnvironment(shinySettings,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(incidenceRatesModule, args = list(
    id = "testIncidenceRates", #Any string is ok?
    dataSource = dataSource,
    cohortTable = cohort,
    selectedCohorts = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    cohortIds = shiny::reactive({c(14906, 14907)})
  ), {
    ## input tests will go here
    session$setInputs(
      irStratification = c("Age", "Calendar Year", "Sex"),
      minPersonYear = 0,
      minSubjetCount = 0 #spelling error in the module
    )
    
    checkmate::expect_data_frame(incidenceRateData())
    
  })
})
