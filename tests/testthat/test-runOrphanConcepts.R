# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

for (nm in names(testServers)) {
  # nm <- "sqlite"
  # nm <- "duckdb"
  # nm <- "postgresql"
  # nm <- "sql_server"
  # nm <- "oracle"
  server <- testServers[[nm]]

  # Params
  cdmDatabaseSchema <- server$cdmDatabaseSchema
  cohortDefinitionSet <- server$cohortDefinitionSet
  tempEmulationSchema <- server$cdmDatabaseSchema
  vocabularyDatabaseSchema <- server$cdmDatabaseSchema
  cohorts <- server$cohortDefinitionSet
  exportFolder <- getUniqueTempDir()
  minCellCount <- 5
  databaseId <- "myDB"
  conceptCountsDatabaseSchema <- server$cohortDatabaseSchema
  conceptCountsTable <- "concept_counts"
  conceptCountsTableIsTemp <- FALSE
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = server$cohortTable)
  cohortDatabaseSchema <- server$cohortDatabaseSchema
  cohortTable <- server$cohortTable
  # incremental <- FALSE
  incremental <- TRUE
  conceptIdTable <- "#concept_ids"
  recordKeepingFile <- file.path(exportFolder, "CreatedDiagnostics.csv")

  # Tests

  test_that(paste("test run orphan codes concept table", nm), {
    connection <- DatabaseConnector::connect(server$connectionDetails)
    exportFolder <- getUniqueTempDir()
    recordKeepingFile <- file.path(exportFolder, "CreatedDiagnostics.csv")
    dir.create(exportFolder)
    # CreateConceptcounts table

    CohortDiagnostics::createConceptCountsTable(connection = connection,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                tempEmulationSchema = NULL,
                                                conceptCountsTable = "concept_counts",
                                                conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                                conceptCountsTableIsTemp = FALSE,
                                                removeCurrentTable = TRUE)

    # Instantiate Unique ConceptSets

    runResolvedConceptSets(connection = connection,
                           cohortDefinitionSet = cohortDefinitionSet,
                           databaseId = databaseId,
                           exportFolder = exportFolder,
                           minCellCount = minCellCount,
                           vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                           tempEmulationSchema = tempEmulationSchema)

    runOrphanConcepts(connection = connection,
                      tempEmulationSchema = tempEmulationSchema,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      vocabularyDatabaseSchema = cdmDatabaseSchema,
                      databaseId = databaseId,
                      cohorts = cohorts,
                      exportFolder = exportFolder,
                      minCellCount = minCellCount,
                      conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                      conceptCountsTable = conceptCountsTable,
                      cohortDatabaseSchema = cohortDatabaseSchema,
                      cohortTable = cohortTable,
                      incremental = incremental,
                      conceptIdTable = conceptIdTable
                      )

    # Check cohort_inc_result
    expect_true(file.exists(file.path(exportFolder, "orphan_concept.csv")))
    orphanResult <- read.csv(file.path(exportFolder, "orphan_concept.csv"))
    expect_equal(colnames(orphanResult), c("cohort_id",
                                           "concept_set_id",
                                           "database_id",
                                           "concept_id",
                                           "concept_count",
                                           "concept_subjects"))
    expect_equal(unique(orphanResult$database_id), databaseId)
    expect_true(all(orphanResult$cohort_id %in% server$cohortDefinitionSet$cohortId))

    # Check cohort_inc_stats
    expect_true(file.exists(file.path(exportFolder, "resolved_concepts.csv")))
    resolvedResult <- read.csv(file.path(exportFolder, "resolved_concepts.csv"))
    expect_equal(colnames(resolvedResult), c("cohort_id", "concept_set_id", "concept_id", "database_id" ))

    # Check recordKeepingFile
    expect_true(file.exists(recordKeepingFile))
    recordKeeping <- read.csv(recordKeepingFile)
    expect_equal(colnames(recordKeeping), c("cohortId", "task", "checksum" , "timeStamp"))
    expect_equal(unique(recordKeeping$task), "runOrphanConcepts")
    expect_true(all(recordKeeping$cohortId %in% server$cohortDefinitionSet$cohortId))

    unlink(exportFolder, recursive = TRUE)

    checkConceptCountsTableExists <- DatabaseConnector::dbExistsTable(connection,
                                                                      name = conceptCountsTable,
                                                                      databaseSchema = conceptCountsDatabaseSchema)
    # always recreate the inst_concept_sets table
    sql <- "TRUNCATE TABLE @work_database_schema.@concept_counts_table; DROP TABLE @work_database_schema.@concept_counts_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      tempEmulationSchema = tempEmulationSchema,
      concept_counts_table = conceptCountsTable,
      work_database_schema  = conceptCountsDatabaseSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )

    if (nm == "oracle") {
      sql <- "TRUNCATE TABLE #inst_concept_sets; DROP TABLE #inst_concept_sets;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql,
        tempEmulationSchema = tempEmulationSchema,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )

      sql <- "TRUNCATE TABLE #concept_ids; DROP TABLE #concept_ids;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql,
        tempEmulationSchema = tempEmulationSchema,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
    unlink(exportFolder, recursive = TRUE)
    DatabaseConnector::disconnect(connection)
  })

  test_that(paste("test run orphan codes temp concept counts", nm), {
    connection <- DatabaseConnector::connect(server$connectionDetails)
    exportFolder <- getUniqueTempDir()
    recordKeepingFile <- file.path(exportFolder, "CreatedDiagnostics.csv")
    dir.create(exportFolder)

    # Instantiate Unique ConceptSets
    runResolvedConceptSets(connection = connection,
                           cohortDefinitionSet = cohortDefinitionSet,
                           databaseId = databaseId,
                           exportFolder = exportFolder,
                           minCellCount = minCellCount,
                           vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                           tempEmulationSchema = tempEmulationSchema)

    runOrphanConcepts(connection = connection,
                      tempEmulationSchema = tempEmulationSchema,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      vocabularyDatabaseSchema = cdmDatabaseSchema,
                      databaseId = databaseId,
                      cohorts = cohorts,
                      exportFolder = exportFolder,
                      minCellCount = minCellCount,
                      conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                      conceptCountsTable = conceptCountsTable,
                      cohortDatabaseSchema = cohortDatabaseSchema,
                      cohortTable = cohortTable,
                      incremental = incremental,
                      conceptIdTable = conceptIdTable
                      )

    # Check cohort_inc_result
    expect_true(file.exists(file.path(exportFolder, "orphan_concept.csv")))
    orphanResult <- read.csv(file.path(exportFolder, "orphan_concept.csv"))
    expect_equal(colnames(orphanResult), c("cohort_id",
                                           "concept_set_id",
                                           "database_id",
                                           "concept_id",
                                           "concept_count",
                                           "concept_subjects"))
    expect_equal(unique(orphanResult$database_id), databaseId)
    expect_true(all(orphanResult$cohort_id %in% server$cohortDefinitionSet$cohortId))

    # Check cohort_inc_stats
    expect_true(file.exists(file.path(exportFolder, "resolved_concepts.csv")))
    resolvedResult <- read.csv(file.path(exportFolder, "resolved_concepts.csv"))
    expect_equal(colnames(resolvedResult), c("cohort_id", "concept_set_id", "concept_id", "database_id" ))

    # Check recordKeepingFile
    expect_true(file.exists(recordKeepingFile))
    recordKeeping <- read.csv(recordKeepingFile)
    expect_equal(colnames(recordKeeping), c("cohortId", "task", "checksum" , "timeStamp"))
    expect_equal(unique(recordKeeping$task), "runOrphanConcepts")
    expect_true(all(recordKeeping$cohortId %in% server$cohortDefinitionSet$cohortId))

    unlink(exportFolder, recursive = TRUE)
    DatabaseConnector::disconnect(connection)
  })
}
