library(testthat)
library(dplyr)

test_that("saveDatabaseMetaData works as expected", {
    exportFolder <- tempfile()
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    with_mocked_bindings(
        {
            CohortDiagnostics:::saveDatabaseMetaData(
                databaseId = "test",
                databaseName = "Test DB",
                databaseDescription = "Description",
                exportFolder = exportFolder,
                minCellCount = 5,
                vocabularyVersionCdm = "v1",
                vocabularyVersion = "v2"
            )
        },
        makeDataExportable = function(x, ...) x,
        writeToCsv = function(data, fileName, ...) {
            readr::write_csv(data, fileName)
        },
        .package = "CohortDiagnostics"
    )

    expect_true(file.exists(file.path(exportFolder, "database.csv")))
    data <- readr::read_csv(file.path(exportFolder, "database.csv"), col_types = readr::cols())
    expect_equal(data$databaseId, "test")
    expect_equal(data$databaseName, "Test DB")
})

test_that("getVocabularyVersion works as expected", {
    mockConn <- mockDatabaseConnection()
    mockResult <- dplyr::tibble(vocabularyVersion = "v123")

    with_mocked_bindings(
        {
            version <- CohortDiagnostics:::getVocabularyVersion(mockConn, "cdm")
        },
        renderTranslateQuerySql = function(...) mockResult,
        .package = "DatabaseConnector"
    )

    expect_equal(version, "v123")
})
