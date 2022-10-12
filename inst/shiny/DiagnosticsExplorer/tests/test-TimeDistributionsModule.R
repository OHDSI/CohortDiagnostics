test_that("Time Distribution Page", {
  initializeEnvironment(shinySettings,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  
  # Environment should have initialized
  expect_true(exists("dataSource"))
  
  shiny::testServer(timeDistributionsModule, args = list(
    id = "testTimeDistributions", #Any string is ok?
    dataSource = dataSource,
    cohortTable = cohort,
    selectedCohorts = shiny::reactive("Any String"),
    selectedDatabaseIds = shiny::reactive("Eunomia"),
    cohortIds = shiny::reactive({c(14906, 14907)}),
    databaseTable = database
  ), {
    ## input tests will go here
    session$setInputs(
       
    )
    
    
    # Checking data type of each column of output matches what it should be
    checkmate::expect_numeric(timeDistributionData()$cohortId)
    checkmate::expect_character(timeDistributionData()$databaseId)
    checkmate::expect_character(timeDistributionData()$timeMetric)
    checkmate::expect_numeric(timeDistributionData()$averageValue)
    checkmate::expect_numeric(timeDistributionData()$standardDeviation)
    checkmate::expect_numeric(timeDistributionData()$minValue)
    checkmate::expect_numeric(timeDistributionData()$p10Value)
    checkmate::expect_numeric(timeDistributionData()$p25Value)
    checkmate::expect_numeric(timeDistributionData()$medianValue)
    checkmate::expect_numeric(timeDistributionData()$p75Value)
    checkmate::expect_numeric(timeDistributionData()$p90Value)
    checkmate::expect_numeric(timeDistributionData()$maxValue)
    

    
  })
})
