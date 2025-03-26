library(testthat)

# check makeDataExportable function
test_that("Check function makeDataExportable", {
  cohortCountTableCorrect <- dplyr::tibble(
    cohortId = 3423,
    cohortEntries = 432432,
    cohortSubjects = 34234,
    databaseId = "ccae"
  )
  cohortCountTableCorrect <-
    CohortDiagnostics:::makeDataExportable(
      x = cohortCountTableCorrect,
      tableName = "cohort_count"
    )
  cohortCountTableCorrectNames <-
    SqlRender::camelCaseToSnakeCase(names(cohortCountTableCorrect)) %>% sort()

  resultsDataModel <- getResultsDataModelSpecifications() %>%
    dplyr::filter(tableName == "cohort_count") %>%
    dplyr::select(columnName) %>%
    dplyr::pull() %>%
    sort()

  expect_true(identical(cohortCountTableCorrectNames, resultsDataModel))

  cohortCountTableInCorrect <- dplyr::tibble(
    cohortIdXXX = 3423,
    cohortEntryXXX = 432432,
    cohortSubjectsXXX = 34234
  )

  expect_error(
    CohortDiagnostics:::makeDataExportable(
      x = cohortCountTableInCorrect,
      tableName = "cohort_count"
    )
  )

  cohortCountTableCorrectDuplicated <-
    dplyr::bind_rows(
      cohortCountTableCorrect,
      cohortCountTableCorrect
    )
  expect_error(
    CohortDiagnostics:::makeDataExportable(
      x = cohortCountTableCorrectDuplicated,
      tableName = "cohort_count"
    )
  )
})

test_that("timeExecutions function", {
  readr::local_edition(1)
  temp <- tempfile()
  on.exit(unlink(temp, force = TRUE, recursive = TRUE))
  dir.create(temp)

  # Basic test
  timeExecution(
    exportFolder = temp,
    taskName = "test_task1",
    cohortIds = c(1, 2, 3, 4),
    expr = {
      Sys.sleep(0.001)
    }
  )
  expectedFilePath <- file.path(temp, "executionTimes.csv")
  checkmate::expect_file_exists(expectedFilePath)
  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 1, ncols = 5)

  expect_false(all(is.na(result$startTime)))
  expect_false(all(is.na(result$executionTime)))

  # Test append
  timeExecution(
    exportFolder = temp,
    taskName = "test_task2",
    cohortIds = NULL,
    expr = {
      Sys.sleep(0.001)
    }
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 2, ncols = 5)

  # Parent string
  timeExecution(
    exportFolder = temp,
    taskName = "test_task3",
    parent = "testthat",
    cohortIds = NULL,
    expr = {
      Sys.sleep(0.001)
    }
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 3, ncols = 5)

  # custom start/end times
  timeExecution(
    exportFolder = temp,
    taskName = "test_task4",
    parent = "testthat",
    cohortIds = NULL,
    start = "foo",
    execTime = "Foo"
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 4, ncols = 5)

  timeExecution(
    exportFolder = temp,
    taskName = "test_task5",
    parent = "testthat",
    cohortIds = NULL,
    start = Sys.time()
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 5, ncols = 5)
  expect_false(all(is.na(result$startTime)))
})

test_that("enforceMinCellValue replaces values below minimum with negative of minimum", {
  data <- data.frame(a = c(1, 2, 3, 4, 5))
  minValues <- 3
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(-3, -3, 3, 4, 5))
})

test_that("enforceMinCellValue does not replace NA values", {
  data <- data.frame(a = c(1, 2, NA, 4, 5))
  minValues <- 3
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(-3, -3, NA, 4, 5))
})

test_that("enforceMinCellValue does not replace zero values", {
  data <- data.frame(a = c(0, 2, 3, 4, 5))
  minValues <- 3
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(0, -3, 3, 4, 5))
})

test_that("enforceMinCellValue works with vector of minimum values", {
  data <- data.frame(a = c(1, 2, 3, 4, 5))
  minValues <- c(1, 2, 3, 4, 5)
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(1, 2, 3, 4, 5))
})

test_that("Creating and checking externalConceptCounts table", {
  message(paste("testing createConceptCountsTable on", dbms))
  
    conceptCountsTable <- "concept_counts"
    
    createConceptCountsTable(connectionDetails = connectionDetails,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             tempEmulationSchema = NULL,
                             conceptCountsTable = conceptCountsTable,
                             conceptCountsDatabaseSchema = cdmDatabaseSchema,
                             conceptCountsTableIsTemp = FALSE,
                             removeCurrentTable = TRUE)
    
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    conceptCounts <- renderTranslateQuerySql(connection, "SELECT * FROM @cdmDatabaseSchema.@conceptCountsTable limit 1;",
                                             cdmDatabaseSchema = cdmDatabaseSchema,
                                             conceptCountsTable = conceptCountsTable)
    
    expect_equal(tolower(colnames(conceptCounts)), c( "concept_id", "concept_count", "concept_subjects", "vocabulary_version"))
  
    # Checking vocab version matches 
    useExternalConceptCountsTable <- TRUE
    conceptCountsTable <- conceptCountsTable
    dataSourceInfo <- getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = cdmDatabaseSchema)
    vocabVersion <- dataSourceInfo$vocabularyVersion
    vocabVersionExternalConceptCountsTable <- renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT DISTINCT vocabulary_version FROM @work_database_schema.@concept_counts_table;",
      work_database_schema = cdmDatabaseSchema,
      concept_counts_table = conceptCountsTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = getOption("sqlRenderTempEmulationSchena")
      )
    DatabaseConnector::disconnect(connection)
    expect_equal(vocabVersion, vocabVersionExternalConceptCountsTable[1,1])
})



