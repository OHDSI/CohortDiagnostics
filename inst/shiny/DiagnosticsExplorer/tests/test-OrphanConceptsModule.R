test_that("Orphan Concepts Page", {
  initializeEnvironment(shinySettings,
                        table1SpecPath = table1SpecPath,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(orphanConceptsModule, args = list(
    id = "testOrphanConcepts", #Any string is ok?
    dataSource = dataSource,
    selectedCohorts = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    targetCohortId = shiny::reactive({c(14906)}),
    selectedConceptSets = NULL,
    conceptSetIds = shiny::reactive({c(0)})
  ), {
    ## input tests will go here
    session$setInputs(
      
    )
    
    # Checking to see if all of the data types outputted are as expected
    checkmate::expect_numeric(orphanConceptsDataReactive()$cohortId)
    checkmate::expect_numeric(orphanConceptsDataReactive()$conceptSetId)
    checkmate::expect_character(orphanConceptsDataReactive()$databaseId)
    checkmate::expect_numeric(orphanConceptsDataReactive()$conceptId)
    checkmate::expect_numeric(orphanConceptsDataReactive()$conceptCount)
    checkmate::expect_numeric(orphanConceptsDataReactive()$conceptSubjects)
    checkmate::expect_character(orphanConceptsDataReactive()$conceptSetName)
    checkmate::expect_character(orphanConceptsDataReactive()$conceptName)
    checkmate::expect_character(orphanConceptsDataReactive()$vocabularyId)
    checkmate::expect_character(orphanConceptsDataReactive()$conceptCode)
    checkmate::expect_character(orphanConceptsDataReactive()$standardConcept)
    
  })
})