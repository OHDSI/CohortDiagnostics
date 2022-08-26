## Tests for annotation module
test_that("Post annotation functions", {
  connectionPool <- getConnectionPool(connectionDetails)
  on.exit({
    pool::poolClose(pool = connectionPool)
  })

  testDataSource <- createDatabaseDataSource(
    connection = connectionPool,
    resultsDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    dbms = "sqlite"
  )

  renderTranslateExecuteSql(testDataSource, "DELETE FROM ANNOTATION_LINK;")
  renderTranslateExecuteSql(testDataSource, "DELETE FROM ANNOTATION;")

  # Check the retreval functions work outside of shiny
  result <- getAnnotationResult(testDataSource,
                                "testAnnotationServer",
                                c(17492, 18342, 17720),
                                c("Eunomia"))
  checkmate::assert_null(x = result)

  # Post test annotation
  postAnnotationResult(dataSource = testDataSource,
                       diagnosticsId = "testAnnotationServer",
                       cohortIds = c(17492),
                       databaseIds = c("Eunomia"),
                       annotation = "TEST annotation",
                       createdBy = "Test user")

  result <- getAnnotationResult(testDataSource,
                                "testAnnotationServer",
                                c(17492),
                                c("Eunomia"))

  # Make sure result matches the input
  checkmate::expect_data_frame(result$annotation, nrows = 1)
  checkmate::expect_data_frame(result$annotationLink, nrows = 1)
  expect_equal(result$annotationLink$cohortId[[1]], 17492)
  expect_equal(result$annotationLink$databaseId[[1]], "Eunomia")
  expect_equal(result$annotation$annotation[[1]], "TEST annotation")
  expect_equal(result$annotation$createdBy[[1]], "Test user")

  renderTranslateExecuteSql(testDataSource, "DELETE FROM ANNOTATION_LINK")
  renderTranslateExecuteSql(testDataSource, "DELETE FROM ANNOTATION")
})

test_that("Annotation shiny server functions", {
  initializeEnvironment(shinySettings,
                        table1SpecPath = table1SpecPath,
                        dataModelSpecificationsPath = dataModelSpecificationsPath)
  id <-"testAnnotationServer"
  shiny::testServer(annotationModule, args = list(
    id = id,
    dataSource = dataSource,
    activeLoggedInUser = shiny::reactiveVal("test-user"),
    selectedDatabaseIds = shiny::reactive(c("Eunomia")),
    selectedCohortIds = shiny::reactive(c(17492, 18342, 17720)),
    cohortTable = cohort,
    databaseTable = database,
    postAnnotaionEnabled = shiny::reactive(TRUE)
  ), {

    session$setInputs(
      targetCohort = NULL,
      database = "Eunomia"
    )
    params <- getParametersToPostAnnotation()
    expect_equal(params$database, "Eunomia")

    # post an entry
    postAnnotationResult(dataSource,
                         id,
                         selectedCohortIds(),
                         selectedDatabaseIds(),
                         "TEST annotation",
                         "Test user")
    # check results are valid
    results <- getAnnotationResult(dataSource,
                                "testAnnotationServer",
                                   selectedCohortIds(),
                                   selectedDatabaseIds())
    # Make sure result matches the input
    checkmate::expect_data_frame(results$annotation, nrows = 1)
    checkmate::expect_data_frame(results$annotationLink, nrows = 3)
  })
})