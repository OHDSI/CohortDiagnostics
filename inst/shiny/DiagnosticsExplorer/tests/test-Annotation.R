## Tests for annotation module

source("../R/Annotation.R")
source("../R/StartUpScripts.R")
source("../R/Results.R")
source("../R/ResultRetrieval.R")

test_that("Posting annotation works", {
  connectionPool <- getConnectionPool(connectionDetails)
  dataModelSpecifications <-
    read.csv("../resultsDataModelSpecification.csv")

  on.exit({
    pool::poolClose(pool = connectionPool)
  })

  dataSource <- createDatabaseDataSource(
    connection = connectionPool,
    resultsDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    dbms = "sqlite"
  )

  renderTranslateExecuteSql(dataSource, "DELETE FROM ANNOTATION_LINK")
  renderTranslateExecuteSql(dataSource, "DELETE FROM ANNOTATION")

  # Check the retreval functions work outside of shiny
  result <- getAnnotationResult(dataSource,
                                "testAnnotationServer",
                                c(17492, 18342, 17720),
                                c("Eunomia"))

  checkmate::expect_list(result)
  checkmate::expect_data_frame(result$annotation)
  checkmate::expect_data_frame(result$annotationLink)

  # Post test annotation
  postAnnotationResult(dataSource,
                       "testAnnotationServer",
                       c(17492),
                       c("Eunomia"),
                       "TEST annotation",
                       "Test user")

  result <- getAnnotationResult(dataSource,
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

  renderTranslateExecuteSql(dataSource, "DELETE FROM ANNOTATION_LINK")
  renderTranslateExecuteSql(dataSource, "DELETE FROM ANNOTATION")

  initializeTables(dataSource, dataModelSpecifications)

  shiny::testServer(annotationModule, args = list(
    id = "testAnnotationServer",
    dataSource = dataSource,
    resultsDatabaseSchema = "main",
    activeLoggedInUser = shiny::reactiveVal("test-user"),
    selectedDatabaseIds = shiny::reactive(c("Eunomia")),
    selectedCohortIds = shiny::reactive(c(17492, 18342, 17720)),
    cohort = cohort,
    postAnnoataionEnabled = shiny::reactive(TRUE)
  ), {

    session$setInputs(
      targetCohort = NULL,
      database = "Eunomia"
    )
    expect_null(getAnnotationReactive())
    params <- getParametersToPostAnnotation()
    expect_equal(params$database, "Eunomia")

    # post an entry
    postAnnotationResult(dataSource,
                     "testAnnotationServer",
                     c(17492),
                     c("Eunomia"),
                     "TEST annotation",
                     "Test user")

    # check results are valid
    results <- getAnnotationReactive()
    # Make sure result matches the input
    checkmate::expect_data_frame(result$annotation, nrows = 1)
    checkmate::expect_data_frame(result$annotationLink, nrows = 1)
  })
})