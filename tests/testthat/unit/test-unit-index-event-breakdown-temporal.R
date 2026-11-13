# Unit tests for temporal index event breakdown
# Issue #1197: Time series of index event breakdown
#
# Tests that the CohortEntryBreakdown.sql template is rendered with
# cohort_start_year grouping correctly.

library(CohortDiagnostics)
library(dplyr)

test_that("CohortEntryBreakdown SQL renders with cohort_start_year in SELECT", {
  # Use the raw SqlRender::render to avoid loadRenderTranslateSql's packageName check
  sqlTemplate <- SqlRender::readSql(
    system.file("sql", "sql_server", "CohortEntryBreakdown.sql",
      package = "CohortDiagnostics"
    )
  )
  sql <- SqlRender::render(sqlTemplate,
    cdm_database_schema = "cdm",
    vocabulary_database_schema = "vocab",
    cohort_database_schema = "cohort",
    cohort_table = "cohort",
    cohort_id = 1,
    domain_table = "condition_occurrence",
    domain_start_date = "condition_start_date",
    domain_concept_id = "condition_concept_id",
    domain_source_concept_id = "condition_source_concept_id",
    use_source_concept_id = FALSE,
    primary_codeset_ids = "1, 2",
    concept_set_table = "#inst_concept_sets",
    store = TRUE,
    store_table = "#breakdown",
    warnOnMissingParameters = FALSE
  )

  # The rendered SQL should include cohort_start_year in the output
  expect_true(grepl("cohort_start_year", sql, fixed = TRUE))

  # The rendered SQL should include YEAR(cohort_start_date) in the inner SELECT
  expect_true(grepl("YEAR(cohort_start_date)", sql, fixed = TRUE))
})

test_that("CohortEntryBreakdown SQL renders with source concepts and cohort_start_year", {
  sqlTemplate <- SqlRender::readSql(
    system.file("sql", "sql_server", "CohortEntryBreakdown.sql",
      package = "CohortDiagnostics"
    )
  )
  sql <- SqlRender::render(sqlTemplate,
    cdm_database_schema = "cdm",
    vocabulary_database_schema = "vocab",
    cohort_database_schema = "cohort",
    cohort_table = "cohort",
    cohort_id = 1,
    domain_table = "condition_occurrence",
    domain_start_date = "condition_start_date",
    domain_concept_id = "condition_concept_id",
    domain_source_concept_id = "condition_source_concept_id",
    use_source_concept_id = TRUE,
    primary_codeset_ids = "1, 2",
    concept_set_table = "#inst_concept_sets",
    store = TRUE,
    store_table = "#breakdown",
    warnOnMissingParameters = FALSE
  )

  # All three UNIONed sub-queries should have cohort_start_year
  yearMatches <- gregexpr("YEAR\\(cohort_start_date\\)", sql)[[1]]
  expect_true(length(yearMatches) >= 3)

  # The outer GROUP BY should include cohort_start_year
  expect_true(grepl("cohort_start_year", sql, fixed = TRUE))
})

test_that("resultsDataModelSpecification includes cohortStartYear column", {
  spec <- CohortDiagnostics:::getResultsDataModelSpecifications()

  iebSpec <- spec %>%
    dplyr::filter(.data$tableName == "index_event_breakdown")

  # column_name values are kept in snake_case in the spec
  columnNames <- iebSpec %>% dplyr::pull(.data$columnName)

  expect_true("cohort_start_year" %in% columnNames)
  expect_true("concept_id" %in% columnNames)
  expect_true("cohort_id" %in% columnNames)

  # Regression guard (found via demo data): cohort_start_year must be part of
  # the primary key. Without it, multiple years for one concept are flagged as
  # duplicate primary keys by makeDataExportable() and the run aborts.
  pkColumns <- iebSpec %>%
    dplyr::filter(.data$primaryKey == "Yes") %>%
    dplyr::pull(.data$columnName) %>%
    sort()

  expect_equal(
    pkColumns,
    sort(c(
      "concept_id", "cohort_id", "database_id",
      "domain_field", "domain_table", "cohort_start_year"
    ))
  )
})

test_that("multiple years for one concept are a valid (non-duplicate) primary key", {
  # Reproduces the demo-data failure mode directly through makeDataExportable.
  data <- dplyr::tibble(
    conceptId = c(201826, 201826, 201826),
    cohortStartYear = c(2020L, 2021L, 2022L),
    conceptCount = c(10, 20, 30),
    subjectCount = c(10, 20, 30),
    cohortId = 1,
    databaseId = "test",
    domainField = "condition_concept_id",
    domainTable = "condition_occurrence"
  )

  # Should NOT throw: rows differ by cohortStartYear.
  expect_no_error(
    CohortDiagnostics:::makeDataExportable(
      x = data,
      tableName = "index_event_breakdown",
      minCellCount = 0,
      databaseId = "test"
    )
  )

  # But a true duplicate (same year twice) must still be caught.
  dupData <- rbind(data, data[1, ])
  expect_error(
    CohortDiagnostics:::makeDataExportable(
      x = dupData,
      tableName = "index_event_breakdown",
      minCellCount = 0,
      databaseId = "test"
    ),
    "duplicates found in primary key"
  )
})

test_that("index event breakdown fallback tibble includes cohortStartYear", {
  data <- dplyr::tibble(
    conceptId = numeric(),
    conceptCount = numeric(),
    subjectCount = numeric(),
    cohortId = numeric(),
    databaseId = character(),
    domainField = character(),
    domainTable = character(),
    cohortStartYear = integer()
  )

  expect_true("cohortStartYear" %in% colnames(data))
  expect_equal(nrow(data), 0)
  expect_type(data$cohortStartYear, "integer")
})

test_that("index event breakdown with cohort_start_year groups results by year", {
  mockData <- dplyr::tibble(
    domainTable = c("condition_occurrence", "condition_occurrence"),
    domainField = c("condition_concept_id", "condition_concept_id"),
    conceptId = c(201826, 201826),
    cohortStartYear = c(2020L, 2021L),
    conceptCount = c(50, 30),
    subjectCount = c(45, 25),
    cohortId = c(1, 1),
    databaseId = c("test", "test")
  )

  yearGroups <- mockData %>%
    dplyr::group_by(.data$conceptId, .data$cohortStartYear) %>%
    dplyr::summarise(n = dplyr::n(), .groups = "drop")

  expect_equal(nrow(yearGroups), 2)
  expect_equal(sort(yearGroups$cohortStartYear), c(2020, 2021))
  expect_equal(mockData$conceptCount[mockData$cohortStartYear == 2020], 50)
  expect_equal(mockData$conceptCount[mockData$cohortStartYear == 2021], 30)
})

test_that("every union branch groups by the concept column it selects", {
  # Spark/Databricks rejects a column that is selected without being in the
  # GROUP BY ("MISSING_AGGREGATION"), which postgres silently allows. Assert
  # the two always match, for every dialect, so a copy/paste slip cannot reach
  # a platform that is only tested downstream.
  template <- SqlRender::readSql(
    system.file("sql", "sql_server", "CohortEntryBreakdown.sql",
      package = "CohortDiagnostics"
    )
  )

  # oracle reformats the aliased columns, so the simple line matching below
  # cannot pair them up; the three dialects used in practice are sufficient
  for (dialect in c("sql server", "postgresql", "spark")) {
    rendered <- SqlRender::render(template,
      cdm_database_schema = "cdm",
      vocabulary_database_schema = "vocab",
      cohort_database_schema = "results",
      cohort_table = "cohort",
      cohort_id = 1,
      domain_table = "visit_occurrence",
      domain_start_date = "visit_start_date",
      domain_concept_id = "visit_concept_id",
      domain_source_concept_id = "visit_source_concept_id",
      use_source_concept_id = TRUE,
      primary_codeset_ids = "1",
      concept_set_table = "cs",
      store = TRUE,
      store_table = "bd",
      warnOnMissingParameters = FALSE
    )
    sql <- SqlRender::translate(rendered, targetDialect = dialect)
    lines <- strsplit(sql, "\n", fixed = TRUE)[[1]]

    selected <- regmatches(
      lines[grep("AS concept_id", lines, fixed = TRUE)],
      gregexpr("visit_(source_)?concept_id AS concept_id", lines[grep("AS concept_id", lines, fixed = TRUE)])
    )
    selected <- sub(" AS concept_id$", "", unlist(selected))
    grouped <- trimws(lines[grep("GROUP BY visit_", lines)])

    expect_equal(length(selected), 6, info = dialect)
    expect_equal(length(grouped), 6, info = dialect)

    for (i in seq_along(selected)) {
      expect_true(
        grepl(selected[[i]], grouped[[i]], fixed = TRUE),
        info = paste(dialect, "branch", i, "selects", selected[[i]], "but groups by", grouped[[i]])
      )
    }
  }
})
