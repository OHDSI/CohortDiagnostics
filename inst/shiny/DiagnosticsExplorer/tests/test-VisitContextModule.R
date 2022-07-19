test_that("Visit context page", {
  initializeEnvironment(shinySettings,
                        table1SpecPath = table1SpecPath,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(visitContextModule, args = list(
    id = "testvisitcontext", #Any string is ok?
    dataSource = dataSource,
    cohortTable = cohort,
    databaseTable = database,
    selectedCohort = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    targetCohortId = shiny::reactive({c(14906)})
  ), {
    ## input tests will go here
     session$setInputs(
       visitContextTableFilters = "All" 
      )
    
    # Checking to see if a dataframe is returned and all the elements are of the 
    # correct datatype
    checkmate::expect_data_frame(getVisitContextData())
    checkmate::expect_data_frame(getVisitContexDataEnhanced())
    checkmate::expect_character(getVisitContexDataEnhanced()$databaseId)
    checkmate::expect_character(getVisitContexDataEnhanced()$visitConceptName)
    checkmate::expect_numeric(getVisitContexDataEnhanced()$Before)
    checkmate::expect_numeric(getVisitContexDataEnhanced()$During)
    checkmate::expect_numeric(getVisitContexDataEnhanced()$Simultaneous)
    checkmate::expect_numeric(getVisitContexDataEnhanced()$After)
  })
})
