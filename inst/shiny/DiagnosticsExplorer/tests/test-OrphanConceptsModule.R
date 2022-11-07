test_that("Orphan Concepts Page", {
  initializeEnvironment(shinySettings,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(orphanConceptsModule, args = list(
    id = "testOrphanConcepts", #Any string is ok?
    dataSource = dataSource,
    selectedCohorts = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    targetCohortId = shiny::reactive({c(14906)}),
    selectedConceptSets = shiny::reactiveVal(NULL),
    conceptSetIds = shiny::reactive({c(0)})
  ), {
    ## input tests will go here
    session$setInputs(
      orphanConceptsType = "Non Standard Only"
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
    
    
    
    # Creating my own testing function to see if standard concepts are only present when called upon
    checkForConceptType <- function(checkTable) {
      if (input$orphanConceptsType == "Standard Only") {
        return (sum(checkTable$standardConcept == "S") == length(checkTable$standardConcept))
      } else if (input$orphanConceptsType == "Non Standard Only") {
        return (sum(checkTable$standardConcept != "S" | is.na(checkTable$standardConcept)) == length(checkTable$standardConcept))
      }
    }
    
    
    # Converting the previous function to a checkmate package expectation test
    expectConceptType = function(checkTable, info = NULL, label = NULL) {
      res = checkForConceptType(checkTable)
      checkmate::makeExpectation(checkTable, res, info = info, label = label)
    }

    # running the test 
    expectConceptType(filteringStandardConceptsReactive())
  })
})
