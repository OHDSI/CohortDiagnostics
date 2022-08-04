test_that("Cohort Overlap Page", {
  initializeEnvironment(shinySettings,
                        table1SpecPath = table1SpecPath,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(cohortOverlapModule, args = list(
    id = "testCohortOverlap", #Any string is ok?
    dataSource = dataSource,
    selectedCohorts = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    targetCohortId = shiny::reactive({c(14906)}),
    cohortIds = shiny::reactive({c(14906, 14907)}),
    cohortTable = cohort
  ), {
    ## input tests will go here
    session$setInputs(
     
    )
    
    # Just checking to make sure all the input data is following the correct variable types
    checkmate::expect_character(cohortOverlapData()$databaseId)
    checkmate::expect_numeric(cohortOverlapData()$comparatorCohortId)
    checkmate::expect_numeric(cohortOverlapData()$eitherSubjects)
    checkmate::expect_numeric(cohortOverlapData()$tOnlySubjects)
    checkmate::expect_numeric(cohortOverlapData()$cOnlySubjects)
    checkmate::expect_numeric(cohortOverlapData()$bothSubjects)
    checkmate::expect_numeric(cohortOverlapData()$targetCohortId)
    checkmate::expect_numeric(cohortOverlapData()$cInTSubjects)
    checkmate::expect_numeric(cohortOverlapData()$cStartAfterTStart)
    checkmate::expect_numeric(cohortOverlapData()$cStartAfterTEnd)
    checkmate::expect_numeric(cohortOverlapData()$cStartBeforeTStart)
    checkmate::expect_numeric(cohortOverlapData()$cStartBeforeTEnd)
    checkmate::expect_numeric(cohortOverlapData()$cStartOnTStart)
    checkmate::expect_numeric(cohortOverlapData()$cStartOnTEnd)



  })
})

