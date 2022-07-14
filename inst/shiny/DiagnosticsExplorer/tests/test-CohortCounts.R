test_that("Cohort counts page", {
  initializeEnvironment(shinySettings,
                        table1SpecPath = table1SpecPath,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(cohortCountsModule, args = list(
    id = "testcohortcounts", #Any string is ok?
    dataSource = dataSource,
    cohortTable = cohort,
    databaseTable = database,
    selectedCohorts = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    cohortIds = shiny::reactive({c(14906, 14907)})
  ), {
    ## input tests will go here
   # session$setInputs(
   #   irStratification = c("Age", "Calendar Year", "Sex"),
  #    minPersonYear = 0,
  #    minSubjetCount = 0 #spelling error in the module
  #  )
    
    # Checking to see if a dataframe is returned and all the elements are of the 
    # correct datatype
    checkmate::expect_data_frame(getResults())
    checkmate::expect_numeric(getResults()$cohortId)
    checkmate::expect_numeric(getResults()$cohortEntries)
    checkmate::expect_numeric(getResults()$cohortSubjects)
    checkmate::expect_character(getResults()$databaseId)
    checkmate::expect_character(getResults()$shortName)
    
    #print(str(output$cohortCountsTable)) #Not sure why this isnt running
  })
})

