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
  server <- testServers[[nm]]
  
  # Params
  cdmDatabaseSchema <- server$cdmDatabaseSchema
  cohortDefinitionSet <- server$cohortDefinitionSet
  tempEmulationSchema <- server$cdmDatabaseSchema
  vocabularyDatabaseSchema <- server$cdmDatabaseSchema
  cohorts <- server$cohortDefinitionSet
  exportFolder <- file.path(tempdir(), paste0(nm, "_concept"))
  minCellCount <- 5
  databaseId <- "myDB"
  conceptCountsDatabaseSchema <- "main"
  conceptCountsTable <- "concept_counts"
  conceptCountsTableIsTemp <- FALSE
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = server$cohortTable)
  cohortDatabaseSchema <- server$cohortDatabaseSchema
  cohortTable <- server$cohortTable
  incremental <- TRUE
  conceptIdTable <- "#concept_ids"
  
  # Tests
  
  test_that(paste("test run inclusion statistics output", nm), {
    connection <- DatabaseConnector::connect(server$connectionDetails)
    exportFolder <- file.path(tempdir(), paste0(nm, "no_concept"))
    dir.create(exportFolder)
    
    # CreateConceptcounts table
    
    CohortDiagnostics::createConceptCountsTable(connection = connection,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                tempEmulationSchema = NULL,
                                                conceptCountsTable = "concept_counts",
                                                conceptCountsDatabaseSchema = cdmDatabaseSchema,
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
                      conceptIdTable = conceptIdTable,
                      recordKeepingFile = recordKeepingFile,
                      resultsDatabaseSchema = resultsDatabaseSchema)
    
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
    expect_equal(unique(recordKeeping$task), "runInclusionStatistics")
    expect_true(all(recordKeeping$cohortId %in% server$cohortDefinitionSet$cohortId))
    
    unlink(exportFolder)
    
    checkConceptCountsTableExists <- DatabaseConnector::dbExistsTable(connection,
                                                                      name = conceptCountsTable,
                                                                      databaseSchema = cdmDatabaseSchema)
    # always recreate the inst_concept_sets table
    if (checkConceptCountsTableExists) {
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = "TRUNCATE TABLE concept_counts; DROP TABLE concept_counts;",
        tempEmulationSchema = tempEmulationSchema,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
  })
  
  test_that(paste("test run inclusion statistics output", nm), {
    connection <- DatabaseConnector::connect(server$connectionDetails)
    exportFolder <- file.path(tempdir(), paste0(nm, "_no_concept"))
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
                      conceptIdTable = conceptIdTable,
                      recordKeepingFile = recordKeepingFile,
                      resultsDatabaseSchema = resultsDatabaseSchema)
    
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
    expect_equal(unique(recordKeeping$task), "runInclusionStatistics")
    expect_true(all(recordKeeping$cohortId %in% server$cohortDefinitionSet$cohortId))
    
    unlink(exportFolder)
  })
}
