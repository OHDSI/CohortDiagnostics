library(SqlRender)
library(readxl)
library(dplyr)
library(readr)


for (nm in names(testServers)) {

  server <- testServers[[nm]]

  con <- DatabaseConnector::connect(server$connectionDetails)

  exportFolder <- tempfile()

  dir.create(exportFolder)

  test_that(paste("test temporary table #concept_ids creation"), {

    getVisitContext(connection = con,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    tempEmulationSchema = server$tempEmulationSchema,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cohortTable =  server$cohortTable,
                    cohortIds = server$cohortIds,
                    conceptIdTable = "#concept_ids",
                    cdmVersion = 5
                    )

    expect_true(tempTableExists(con, "concept_ids"))

  })


  test_that(paste("test no duplicates in concept_ids table for getVisitContext function"), {

    sql <-  "SELECT * FROM #concept_ids"

    translatedSql <- SqlRender::translate(sql, targetDialect = server$connectionDetails$dbms)

    firstTime <- system.time(
      visitContextResult <- getVisitContext(connection = con,
                                            cdmDatabaseSchema = server$cdmDatabaseSchema,
                                            tempEmulationSchema = server$tempEmulationSchema,
                                            cohortDatabaseSchema = server$cohortDatabaseSchema,
                                            cohortTable =  server$cohortTable,
                                            cohortIds = server$cohortIds,
                                            conceptIdTable = "#concept_ids",
                                            cdmVersion = 5)
    )

    firstResult <- DatabaseConnector::querySql(con, translatedSql)

    secondTime <- system.time(
      visitContextResult <- getVisitContext(connection = con,
                                            cdmDatabaseSchema = server$cdmDatabaseSchema,
                                            tempEmulationSchema = server$tempEmulationSchema,
                                            cohortDatabaseSchema = server$cohortDatabaseSchema,
                                            cohortTable =  server$cohortTable,
                                            cohortIds = server$cohortIds,
                                            conceptIdTable = "#concept_ids",
                                            cdmVersion = 5
                                            )
    )

    secondResult <- DatabaseConnector::querySql(con, translatedSql)

    expect_equal(firstResult, secondResult)

  })

  DatabaseConnector::disconnect(con)
}

# For testing the runVisitContext, there is no need to run it on multiple database systems since no sql other than
# the one included in the getVisitContext is executed.
if ("sqlite" %in% names(testServers)) {
  
  server <- testServers[["sqlite"]]
  con <- DatabaseConnector::connect(server$connectionDetails)
  
  test_that(paste("test that when incremental is FALSE the incremental file is not generated"), {
    
    exportFolder <- tempfile()
    dir.create(exportFolder)
    
    expect_false(file.exists(file.path(exportFolder,"incremental")))
    
    runVisitContext(connection = con,
                    cohortDefinitionSet = server$cohortDefinitionSet,
                    exportFolder = exportFolder,
                    databaseId = nm ,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = FALSE
    )
    
    expect_false(file.exists(file.path(exportFolder,"incremental")))
  })
  
  test_that(paste("test that when incremental is TRUE the incremental file is generated when it doesn't exist"), {
    
    exportFolder <- tempfile()
    dir.create(exportFolder)
    
    expect_false(file.exists(file.path(exportFolder, "incremental")))
    
    runVisitContext(connection = con,
                    cohortDefinitionSet = server$cohortDefinitionSet,
                    exportFolder = exportFolder,
                    databaseId = nm ,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = TRUE
    )
    
    expect_true(file.exists(file.path(exportFolder, "incremental")))
    
  })
  
  
  test_that(paste("test that the output file visit_context.csv is generated and is identical with the output of getVisitContext()"), {
    
    exportFolder <- tempfile()
    dir.create(exportFolder)
    
    getVisitContextResult <- getVisitContext(connection = con,
                                             cdmDatabaseSchema = server$cdmDatabaseSchema,
                                             tempEmulationSchema = server$tempEmulationSchema,
                                             cohortDatabaseSchema = server$cohortDatabaseSchema,
                                             cohortTable =  server$cohortTable,
                                             cohortIds = server$cohortIds,
                                             conceptIdTable = "#concept_ids",
                                             cdmVersion = 5
    )
    
    getVisitContextResult <- unname(getVisitContextResult)
    
    runVisitContext(connection = con,
                    cohortDefinitionSet = server$cohortDefinitionSet,
                    exportFolder = exportFolder,
                    databaseId = nm,
                    cohortTable =  server$cohortTable,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = FALSE
    )
    
    resultCsv <- file.path(exportFolder, "visit_context.csv")
    
    expect_true(file.exists(resultCsv))
    
    runVisitContextResult <- read.csv(resultCsv, header = TRUE, sep = ",")
    runVisitContextResult$database_id <- NULL
    runVisitContextResult <- unname(runVisitContextResult)
    
    expect_equal(getVisitContextResult, runVisitContextResult)
    
  })
  
  
  test_that(paste("test that incremental logic is correct: incremental run for the first time"), {
    
    exportFolder <- tempfile()
    dir.create(exportFolder)
    
    cohortIds <- c(17492)

    runVisitContext(connection = con,
                    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds, useSubsets = FALSE),
                    exportFolder = exportFolder,
                    databaseId = nm,
                    cohortTable =  server$cohortTable,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = TRUE
    )
    
    resultCsv <- file.path(exportFolder, "visit_context.csv")
    
    expect_true(file.exists(resultCsv))
    
    results <- read.csv(resultCsv, header = TRUE, stringsAsFactors = FALSE)
    
    # csv should contain results only from the specified cohort
    expect_equal(unique(results$cohort_id), c(17492))
    
  })
  
  test_that(paste("test that incremental logic is correct: no new cohorts"), {
    
    exportFolder <- tempfile()
    dir.create(exportFolder)
    
    cohortIds <- c(17492)
    
    runVisitContext(connection = con,
                    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds, useSubsets = FALSE),
                    exportFolder = exportFolder,
                    databaseId = nm,
                    cohortTable =  server$cohortTable,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = TRUE
    )
    
    resultCsv <- file.path(exportFolder, "visit_context.csv")
    
    expect_true(file.exists(resultCsv))
    
    results1 <- read.csv(resultCsv, header = TRUE, stringsAsFactors = FALSE)
    
    runVisitContext(connection = con,
                    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds, useSubsets = FALSE),
                    exportFolder = exportFolder,
                    databaseId = nm,
                    cohortTable =  server$cohortTable,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = TRUE
    )
    
    resultCsv <- file.path(exportFolder, "visit_context.csv")
    
    expect_true(file.exists(resultCsv))
    
    results2 <- read.csv(resultCsv, header = TRUE, stringsAsFactors = FALSE)
    
    # csv should contain the same result after the first run and the second run as no new cohorts were added
    expect_equal(results1, results2)
    
  })
  
  test_that(paste("test that incremental logic is correct: output visit_context.csv must contain results for new cohorts"), {
    
    exportFolder <- tempfile()
    dir.create(exportFolder)
    
    cohortIds <- c(17492)
    
    runVisitContext(connection = con,
                    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds, useSubsets = FALSE),
                    exportFolder = exportFolder,
                    databaseId = nm,
                    cohortTable =  server$cohortTable,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = TRUE
    )
    
    resultCsv <- file.path(exportFolder, "visit_context.csv")
    
    expect_true(file.exists(resultCsv))
    
    results1 <- read.csv(resultCsv, header = TRUE, stringsAsFactors = FALSE)
    
    # csv should contain results only from the specified cohort
    expect_equal(unique(results1$cohort_id), c(17492))
    
    cohortIds <- c(17492, 17493)
    
    runVisitContext(connection = con,
                    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds, useSubsets = FALSE),
                    exportFolder = exportFolder,
                    databaseId = nm,
                    cohortTable =  server$cohortTable,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    minCellCount = 0,
                    incremental = TRUE
    )
    
    resultCsv <- file.path(exportFolder, "visit_context.csv")
    
    expect_true(file.exists(resultCsv))
    
    results2 <- read.csv(resultCsv, header = TRUE, stringsAsFactors = FALSE)
    
    # csv should contain results from both runs, hence both cohorts
    expect_equal(unique(results2$cohort_id), c(17492, 17493))
    
  })
}

##### Test cases with custom data #####

test_that(paste("test that the subject counts per cohort, visit concept and visit context are correct"), {

  cohortDataFilePath <- system.file("test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_cohort.xlsx",  package = "CohortDiagnostics")

  patientDataFilePath <- "test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_patientData.json"

  connectionDetailsCustomCDM <- createCustomCdm(patientDataFilePath)


  connection <- DatabaseConnector::connect(connectionDetails = connectionDetailsCustomCDM)

  addCohortTable(connection, cohortDataFilePath)


  visitContextResult <- getVisitContext(connection = connection,
                                        cdmDatabaseSchema = "main",
                                        tempEmulationSchema = "main",
                                        cohortDatabaseSchema = "main",
                                        cohortTable =  "cohort",
                                        cohortIds = list(1,2),
                                        conceptIdTable = "#concept_ids",
                                        cdmVersion = 5
  )

  resultPath <- system.file("test_cases/runVisitContext/testSubjectCounts/expectedResult.csv", package = "CohortDiagnostics")

  resultData <- readr::read_csv(resultPath, col_types = c("numeric", "numeric", "text", "numeric"))

  visitContextResult <- visitContextResult[order(visitContextResult$cohortId, visitContextResult$visitConceptId, visitContextResult$visitContext, visitContextResult$subjects), ]
  visitContextResult <- as.data.frame(lapply(visitContextResult, as.character), stringsAsFactors = FALSE)

  resultData <-  resultData[order(resultData$cohortId, resultData$visitConceptId, resultData$visitContext, resultData$subjects), ]
  resultData <-  as.data.frame(lapply(resultData, as.character), stringsAsFactors = FALSE)

  are_equal <- identical(visitContextResult, resultData)

  expect_true(are_equal)

})

test_that(paste("test that only the new visit_concept_id are inserted into the #concept_ids table"), {

  cohortDataFilePath <- system.file("test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_cohort.csv",  
                                    package = "CohortDiagnostics", 
                                    mustWork = TRUE)

  patientDataFilePath <- "test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_patientData.json"

  connectionDetailsCustomCDM <- createCustomCdm(patientDataFilePath)

  connection <- DatabaseConnector::connect(connectionDetails = connectionDetailsCustomCDM)

  addCohortTable(connection, cohortDataFilePath)

  getVisitContext(connection = connection,
                                cdmDatabaseSchema = "main",
                                tempEmulationSchema = "main",
                                cohortDatabaseSchema = "main",
                                cohortTable =  "cohort",
                                cohortIds = list(1,2),
                                conceptIdTable = "#concept_ids",
                                cdmVersion = 5
                  )

  sql <- "select * from #concept_ids"

  translatedSQL <- translate(sql, targetDialect = "sqlite")

  res1 <- querySql(connection = connection, sql = translatedSQL)


  are_equal <- all(sort(unlist(list(262, 9201))) == sort(unlist(res1$CONCEPT_ID)))

  expect_true(are_equal)

  new_row <- data.frame(
    visit_occurrence_id = 5,
    person_id = 2,
    visit_concept_id = 261,
    visit_start_date = as.Date("2015-01-10"),
    visit_start_datetime = as.POSIXct("2015-01-10 08:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    visit_end_date = as.Date("2015-01-10"),
    visit_end_datetime = as.POSIXct("2015-01-10 18:00:00", format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    visit_type_concept_id = 32817,
    provider_id = 1,
    care_site_id = 1,
    visit_source_value = 0,
    visit_source_concept_id = 0,
    admitting_source_concept_id = 8870,
    admitting_source_value = "TRANSFER FROM HOSPITAL",
    discharge_to_concept_id = 581476,
    discharge_to_source_value = "HOME HEALTH CARE",
    preceding_visit_occurrence_id = 0
  )

  DBI::dbAppendTable(connection, "visit_occurrence", new_row)

  getVisitContext(connection = connection,
                  cdmDatabaseSchema = "main",
                  tempEmulationSchema = "main",
                  cohortDatabaseSchema = "main",
                  cohortTable =  "cohort",
                  cohortIds = list(1,2),
                  conceptIdTable = "#concept_ids",
                  cdmVersion = 5
                  )

  sql <- "select * from #concept_ids"

  translatedSQL <- translate(sql, targetDialect = "sqlite")

  res2 <- querySql(connection = connection, sql = translatedSQL)

  are_equal <- all(sort(unlist(list(262, 9201, 261))) == sort(unlist(res2$CONCEPT_ID)))

  expect_true(are_equal)
})



test_that(paste("test that to infer subject counts per cohort, visit concept, and visit context, visits within 30 days before or after cohort creation are considered"), {

  cohortDataFilePath <- system.file("test_cases/runVisitContext/testSubjectCountsDates/test_getVisitContext_cohort.xlsx",  package = "CohortDiagnostics")

  patientDataFilePath <- "test_cases/runVisitContext/testSubjectCountsDates/test_getVisitContext_patientData.json"

  connectionDetailsCustomCDM <- createCustomCdm(patientDataFilePath)

  connection <- DatabaseConnector::connect(connectionDetails = connectionDetailsCustomCDM)

  addCohortTable(connection, cohortDataFilePath)

  visitContextResult <- getVisitContext(connection = connection,
                                          cdmDatabaseSchema = "main",
                                          tempEmulationSchema = "main",
                                          cohortDatabaseSchema = "main",
                                          cohortTable =  "cohort",
                                          cohortIds = list(1,2),
                                          conceptIdTable = "#concept_ids",
                                          cdmVersion = 5
    )

    resultPath <- system.file("test_cases/runVisitContext/testSubjectCountsDates/expectedResult.csv", 
                              package = "CohortDiagnostics", 
                              mustWork = T)

    resultData <- readr::read_csv(resultPath, col_types = c("numeric", "numeric", "text", "numeric"))

    visitContextResult <- visitContextResult[order(visitContextResult$cohortId, visitContextResult$visitConceptId, visitContextResult$visitContext, visitContextResult$subjects), ]
    visitContextResult <- as.data.frame(lapply(visitContextResult, as.character), stringsAsFactors = FALSE)

    resultData <-  resultData[order(resultData$cohortId, resultData$visitConceptId, resultData$visitContext, resultData$subjects), ]
    resultData <-  as.data.frame(lapply(resultData, as.character), stringsAsFactors = FALSE)

    are_equal <- identical(visitContextResult, resultData)

    expect_true(are_equal)

})

test_that(paste("test that no other cohorts than the ones specified in cohortIds are included in the output"), {

  cohortDataFilePath <- system.file("test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_cohort.xlsx",  package = "CohortDiagnostics")

  patientDataFilePath <- "test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_patientData.json"

  connectionDetailsCustomCDM <- createCustomCdm(patientDataFilePath)

  connection <- DatabaseConnector::connect(connectionDetails = connectionDetailsCustomCDM)

  addCohortTable(connection, cohortDataFilePath)

  visitContextResult <- getVisitContext(connection = connection,
                                          cdmDatabaseSchema = "main",
                                          tempEmulationSchema = "main",
                                          cohortDatabaseSchema = "main",
                                          cohortTable =  "cohort",
                                          cohortIds = list(1),
                                          conceptIdTable = "#concept_ids",
                                          cdmVersion = 5
    )

  print(visitContextResult)
  expect_true(identical(unique(visitContextResult$cohortId), c(1)))

})

test_that(paste("test that when the subjects in the cohort have no visits an empty data frame is returned"), {

  cohortDataFilePath <- system.file("test_cases/runVisitContext/testSubjectCountsNoVisits/test_getVisitContext_cohort.xlsx",  package = "CohortDiagnostics")

  patientDataFilePath <- "test_cases/runVisitContext/testSubjectCountsNoVisits/test_getVisitContext_patientData.json"

  connectionDetailsCustomCDM <- createCustomCdm(patientDataFilePath)

  connection <- DatabaseConnector::connect(connectionDetails = connectionDetailsCustomCDM)

  addCohortTable(connection, cohortDataFilePath)

  sql <- "delete from visit_occurrence;"

  translatedSQL <- translate(sql = sql, targetDialect =  "sqlite")

  executeSql(connection = connection, sql = translatedSQL)

  visitContextResult <- getVisitContext(connection = connection,
                                        cdmDatabaseSchema = "main",
                                        tempEmulationSchema = "main",
                                        cohortDatabaseSchema = "main",
                                        cohortTable =  "cohort",
                                        cohortIds = list(1,2),
                                        conceptIdTable = "#concept_ids",
                                        cdmVersion = 5
  )

  resultPath <- system.file("test_cases/runVisitContext/testSubjectCountsNoVisits/expectedResult.csv", 
                            package = "CohortDiagnostics",
                            mustWork = TRUE)

  resultData <- readr::read_csv(resultPath, col_types = c("numeric", "numeric", "text", "numeric"))

  visitContextResult <- visitContextResult[order(visitContextResult$cohortId, visitContextResult$visitConceptId, visitContextResult$visitContext, visitContextResult$subjects), ]
  visitContextResult <- as.data.frame(lapply(visitContextResult, as.character), stringsAsFactors = FALSE)

  resultData <-  resultData[order(resultData$cohortId, resultData$visitConceptId, resultData$visitContext, resultData$subjects), ]
  resultData <-  as.data.frame(lapply(resultData, as.character), stringsAsFactors = FALSE)

  are_equal <- identical(visitContextResult, resultData)

  expect_true(are_equal)
})
