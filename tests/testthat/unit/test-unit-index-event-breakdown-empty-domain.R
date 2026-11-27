library(testthat)
library(CohortDiagnostics)

test_that("runConceptSetDiagnostics handles index event breakdown when a domain returns zero rows (#1183)", {
  connection <- mockDatabaseConnection()
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE), add = TRUE)
  recordKeepingFile <- file.path(exportFolder, "record.csv")

  cohortJson <- jsonlite::toJSON(
    list(
      ConceptSets = list(
        list(id = 0L, name = "CondSet", expression = list(items = list())),
        list(id = 1L, name = "ObsSet", expression = list(items = list()))
      ),
      PrimaryCriteria = list(
        CriteriaList = list(
          list(ConditionOccurrence = list(CodesetId = 0L)),
          list(Observation = list(CodesetId = 1L))
        )
      )
    ),
    auto_unbox = TRUE
  )

  cohorts <- dplyr::tibble(
    cohortId = 1L,
    cohortName = "Multi-domain cohort",
    json = as.character(cohortJson),
    sql = paste(
      "SELECT 0 as codeset_id (SELECT 201826) C",
      "SELECT 1 as codeset_id (SELECT 999999) C",
      "with primary_events"
    ),
    isSubset = FALSE,
    subsetParent = 1L,
    cohortFullName = "Multi-domain cohort",
    checksum = "abc123"
  )

  # Simulate SQLite/DatabaseConnector returning a non-empty character/numeric
  # tibble for ConditionOccurrence and a 0-row logical-typed tibble for Observation.
  queryCall <- 0L
  exported <- NULL

  local_mocked_bindings(
    makeDataExportable = function(x, tableName, ...) {
      if (identical(tableName, "index_event_breakdown")) {
        exported <<- x
      }
      x
    },
    writeToCsv = function(...) NULL,
    recordTasksDone = function(...) NULL,
    .package = "CohortDiagnostics"
  )

  local_mocked_bindings(
    executeSql = function(...) NULL,
    renderTranslateExecuteSql = function(...) NULL,
    renderTranslateQuerySql = function(connection, sql, ...) {
      if (grepl("inst_concept_sets", sql, fixed = TRUE)) {
        return(data.frame(codesetId = c(0L, 1L), conceptId = c(201826, 999999)))
      }
      queryCall <<- queryCall + 1L
      if (queryCall == 1L) {
        return(data.frame(
          domainTable = "condition_occurrence",
          domainField = "condition_concept_id",
          conceptId = 201826,
          cohortStartYear = -1L,
          conceptCount = 10,
          subjectCount = 8,
          stringsAsFactors = FALSE
        ))
      }
      data.frame(
        domainTable = logical(0),
        domainField = logical(0),
        conceptId = numeric(0),
        cohortStartYear = logical(0),
        conceptCount = logical(0),
        subjectCount = logical(0)
      )
    },
    .package = "DatabaseConnector"
  )

  local_mocked_bindings(
    loadRenderTranslateSql = function(...) "SELECT 1;",
    .package = "SqlRender"
  )

  expect_no_error(
    CohortDiagnostics:::runConceptSetDiagnostics(
      connection = connection,
      tempEmulationSchema = NULL,
      cdmDatabaseSchema = "cdm",
      vocabularyDatabaseSchema = "cdm",
      databaseId = "test_db",
      cohorts = cohorts,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runBreakdownIndexEvents = TRUE,
      exportFolder = exportFolder,
      minCellCount = 0,
      cohortDatabaseSchema = "cohort",
      cohortTable = "cohort",
      incremental = FALSE,
      conceptIdTable = NULL,
      recordKeepingFile = recordKeepingFile
    )
  )

  expect_equal(nrow(exported), 1L)
  expect_equal(exported$domainTable, "condition_occurrence")
  expect_equal(exported$conceptId, 201826)
  expect_equal(exported$conceptCount, 10)
  expect_equal(exported$cohortId, 1L)
  expect_equal(exported$databaseId, "test_db")
})

test_that("runConceptSetDiagnostics handles index event breakdown when all domains return zero rows", {
  connection <- mockDatabaseConnection()
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE), add = TRUE)
  recordKeepingFile <- file.path(exportFolder, "record.csv")

  cohortJson <- jsonlite::toJSON(
    list(
      ConceptSets = list(
        list(id = 0L, name = "CondSet", expression = list(items = list())),
        list(id = 1L, name = "ObsSet", expression = list(items = list()))
      ),
      PrimaryCriteria = list(
        CriteriaList = list(
          list(ConditionOccurrence = list(CodesetId = 0L)),
          list(Observation = list(CodesetId = 1L))
        )
      )
    ),
    auto_unbox = TRUE
  )

  cohorts <- dplyr::tibble(
    cohortId = 1L,
    cohortName = "Empty multi-domain cohort",
    json = as.character(cohortJson),
    sql = paste(
      "SELECT 0 as codeset_id (SELECT 201826) C",
      "SELECT 1 as codeset_id (SELECT 999999) C",
      "with primary_events"
    ),
    isSubset = FALSE,
    subsetParent = 1L,
    cohortFullName = "Empty multi-domain cohort",
    checksum = "def456"
  )

  exported <- NULL

  local_mocked_bindings(
    makeDataExportable = function(x, tableName, ...) {
      if (identical(tableName, "index_event_breakdown")) {
        exported <<- x
      }
      x
    },
    writeToCsv = function(...) NULL,
    recordTasksDone = function(...) NULL,
    .package = "CohortDiagnostics"
  )

  local_mocked_bindings(
    executeSql = function(...) NULL,
    renderTranslateExecuteSql = function(...) NULL,
    renderTranslateQuerySql = function(connection, sql, ...) {
      if (grepl("inst_concept_sets", sql, fixed = TRUE)) {
        return(data.frame(codesetId = c(0L, 1L), conceptId = c(201826, 999999)))
      }
      data.frame(
        domainTable = logical(0),
        domainField = logical(0),
        conceptId = numeric(0),
        cohortStartYear = logical(0),
        conceptCount = logical(0),
        subjectCount = logical(0)
      )
    },
    .package = "DatabaseConnector"
  )

  local_mocked_bindings(
    loadRenderTranslateSql = function(...) "SELECT 1;",
    .package = "SqlRender"
  )

  expect_no_error(
    CohortDiagnostics:::runConceptSetDiagnostics(
      connection = connection,
      tempEmulationSchema = NULL,
      cdmDatabaseSchema = "cdm",
      vocabularyDatabaseSchema = "cdm",
      databaseId = "test_db",
      cohorts = cohorts,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runBreakdownIndexEvents = TRUE,
      exportFolder = exportFolder,
      minCellCount = 0,
      cohortDatabaseSchema = "cohort",
      cohortTable = "cohort",
      incremental = FALSE,
      conceptIdTable = NULL,
      recordKeepingFile = recordKeepingFile
    )
  )

  expect_equal(nrow(exported), 0L)
  expect_true(all(c("conceptId", "domainTable", "domainField", "cohortStartYear") %in% colnames(exported)))
})
