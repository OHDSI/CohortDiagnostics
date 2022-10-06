test_that("Index Event Breakdown Page", {
  initializeEnvironment(shinySettings,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(indexEventBreakdownModule, args = list(
    id = "testindexeventbreakdown", #Any string is ok?
    dataSource = dataSource,
    selectedCohort = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    targetCohortId = shiny::reactive({c(14906)})
  ), {
    ## input tests will go here
    session$setInputs(
      indexEventBreakdownTableRadioButton = "All" 
    )
    
    checkmate::expect_numeric(indexEventBreakDownDataFilteredByRadioButton()$conceptId)
    checkmate::expect_character(indexEventBreakDownDataFilteredByRadioButton()$conceptName)
    checkmate::expect_character(indexEventBreakDownDataFilteredByRadioButton()$domainField)
    checkmate::expect_character(indexEventBreakDownDataFilteredByRadioButton()$vocabularyId)
    #Unsure if the next two are the exact translation of output columns to the ones in data
    checkmate::expect_numeric(indexEventBreakDownDataFilteredByRadioButton()$cohortSubjects) #persons?
    checkmate::expect_numeric(indexEventBreakDownDataFilteredByRadioButton()$cohortEntries) #records?
  })
})
