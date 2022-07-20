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
       visitContextTableFilters = "Simultaneous" 
      )
    
    # Checking to see if a dataframe is returned and all the elements are of the 
    # correct datatype
    checkmate::expect_data_frame(getVisitContextData())
    checkmate::expect_data_frame(getVisitContexDataEnhanced())
    checkmate::expect_character(getVisitContexDataEnhanced()$databaseId)
    checkmate::expect_character(getVisitContexDataEnhanced()$visitConceptName)
    
    # Initializing vectors with column names
    before_vec <- c("databaseId", "visitConceptName", "Before")
    during_vec <- c("databaseId", "visitConceptName", "During")
    simul_vec <- c("databaseId", "visitConceptName", "Simultaneous")
    after_vec <- c("databaseId", "visitConceptName", "After")
    all_vec <- c("databaseId", "visitConceptName","Before", "During", "Simultaneous", "After")
    
    # Checking to see if the appropriate columns are represented in the data table 
    # depending on what filtering selection is utilized
    if (input$visitContextTableFilters == "Before"){
      checkmate::expect_numeric(getVisitContexDataEnhanced()$Before)
      testthat::expect_equal(colnames(getVisitContexDataEnhanced()), before_vec)
    } else if (input$visitContextTableFilters == "During"){
      checkmate::expect_numeric(getVisitContexDataEnhanced()$During)
      testthat::expect_equal(colnames(getVisitContexDataEnhanced()), during_vec)
    } else if (input$visitContextTableFilters == "Simultaneous"){
      checkmate::expect_numeric(getVisitContexDataEnhanced()$Simultaneous)
      testthat::expect_equal(colnames(getVisitContexDataEnhanced()), simul_vec)
    } else if (input$visitContextTableFilters == "After"){
      checkmate::expect_numeric(getVisitContexDataEnhanced()$After)
      testthat::expect_equal(colnames(getVisitContexDataEnhanced()), after_vec)
    } else if (input$visitContextTableFilters == "All"){
      checkmate::expect_numeric(getVisitContexDataEnhanced()$Before)
      checkmate::expect_numeric(getVisitContexDataEnhanced()$During)
      checkmate::expect_numeric(getVisitContexDataEnhanced()$Simultaneous)
      checkmate::expect_numeric(getVisitContexDataEnhanced()$After)
      testthat::expect_equal(colnames(getVisitContexDataEnhanced()), all_vec)
    }
    
    
  })
})
