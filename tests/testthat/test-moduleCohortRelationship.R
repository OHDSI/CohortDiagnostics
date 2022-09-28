test_that("Testing executeCohortRelationshipDiagnostics", {
  skip_if(skipCdmTests, "cdm settings not configured")
  
  # manually create cohort table and load to table
  # for the logic to work - there has to be some overlap of the comparator cohort over target cohort
  # note - we will not be testing offset in this test. it is expected to work as it is a simple substraction
  
  temporalStartDays <- c(0)
  temporalEndDays <- c(0)
  
  targetCohort <- dplyr::tibble(
    cohortDefinitionId = c(1),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1900-01-15")),
    cohortEndDate = c(as.Date("1900-01-31"))
  ) # target cohort always one row
  
  comparatorCohort <- # all records here overlap with targetCohort
    dplyr::tibble(
      cohortDefinitionId = c(10, 10, 10),
      subjectId = c(1, 1, 1),
      cohortStartDate = c(
        as.Date("1900-01-01"),
        # starts before target cohort start
        as.Date("1900-01-22"),
        # starts during target cohort period and ends during target cohort period
        as.Date("1900-01-31")
      ),
      cohortEndDate = c(
        as.Date("1900-01-20"),
        as.Date("1900-01-29"),
        as.Date("1900-01-31")
      )
    )
  
  cohort <- dplyr::bind_rows(
    targetCohort,
    comparatorCohort,
    targetCohort %>%
      dplyr::mutate(cohortDefinitionId = 2),
    comparatorCohort %>%
      dplyr::mutate(cohortDefinitionId = 20)
  )
  
  connectionCohortRelationship <-
    DatabaseConnector::connect(connectionDetails)
  
  # to do - with incremental = FALSE
  with_dbc_connection(connectionCohortRelationship, {
    sysTime <- as.numeric(Sys.time()) * 100000
    tableName <- paste0("cr", sysTime)
    observationTableName <- paste0("op", sysTime)
    
    DatabaseConnector::insertTable(
      connection = connectionCohortRelationship,
      databaseSchema = cohortDatabaseSchema,
      tableName = tableName,
      data = cohort,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = FALSE,
      camelCaseToSnakeCase = TRUE,
      progressBar = FALSE
    )
    
    cohortDefinitionSet <-
      cohort %>%
      dplyr::select(.data$cohortDefinitionId) %>%
      dplyr::distinct() %>%
      dplyr::rename(cohortId = .data$cohortDefinitionId) %>%
      dplyr::rowwise() %>%
      dplyr::mutate(json = RJSONIO::toJSON(list(
        cohortId = .data$cohortId,
        randomString = c(
          sample(x = LETTERS, 5, replace = TRUE),
          sample(x = LETTERS, 4, replace = TRUE),
          sample(LETTERS, 1, replace = TRUE)
        )
      ))) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(sql = json,
                    checksum = CohortDiagnostics:::computeChecksum(.data$json))
    
    
    exportFolder <- tempdir()
    exportFile <- tempfile()
    
    unlink(x = exportFolder,
           recursive = TRUE,
           force = TRUE)
    dir.create(path = exportFolder,
               showWarnings = FALSE,
               recursive = TRUE)
    
    CohortDiagnostics:::executeCohortRelationshipDiagnostics(
      connection = connectionCohortRelationship,
      databaseId = "testDataSourceName",
      exportFolder = exportFolder,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = tableName,
      tempEmulationSchema = NULL,
      cohortDefinitionSet = cohortDefinitionSet %>%
        dplyr::filter(.data$cohortId %in% c(1, 10)),
      temporalCovariateSettings = list(
        temporalStartDays = c(-365, -30),
        temporalEndDays = c(-31, -1)
      ),
      minCellCount = 0,
      recordKeepingFile = paste0(exportFile, "recordKeeping"),
      incremental = TRUE,
      batchSize = 2
    )
    
    recordKeepingFileData <-
      readr::read_csv(file = paste0(exportFile, "recordKeeping"),
                      col_types = readr::cols())
    
    # testing if check sum if written to field called targetChecksum
    testthat::expect_true("targetChecksum" %in% colnames(recordKeepingFileData))
    testthat::expect_true("comparatorChecksum" %in% colnames(recordKeepingFileData))
    testthat::expect_true("checksum" %in% colnames(recordKeepingFileData))
    
    testthat::expect_equal(
      object = recordKeepingFileData %>%
        dplyr::filter(.data$cohortId == 1) %>%
        dplyr::filter(.data$comparatorId == 10) %>%
        dplyr::select(.data$checksum) %>%
        dplyr::pull(.data$checksum),
      expected = recordKeepingFileData %>%
        dplyr::filter(.data$cohortId == 1) %>%
        dplyr::filter(.data$comparatorId == 10) %>%
        dplyr::mutate(
          checksum2 = paste0(.data$targetChecksum,
                             .data$comparatorChecksum)
        ) %>%
        dplyr::pull(.data$checksum2)
    )
    
    
    
    ## testing if subset works
    allCohortIds <- cohortDefinitionSet  %>%
      dplyr::filter(.data$cohortId %in% c(1, 10, 2)) %>%
      dplyr::select(.data$cohortId, .data$checksum) %>%
      dplyr::rename(targetCohortId = .data$cohortId,
                    targetChecksum = .data$checksum) %>%
      dplyr::distinct()
    
    combinationsOfPossibleCohortRelationships <- allCohortIds %>%
      tidyr::crossing(
        allCohortIds %>%
          dplyr::rename(
            comparatorCohortId = .data$targetCohortId,
            comparatorChecksum = .data$targetChecksum
          )
      ) %>%
      dplyr::filter(.data$targetCohortId != .data$comparatorCohortId) %>%
      dplyr::arrange(.data$targetCohortId, .data$comparatorCohortId) %>%
      dplyr::mutate(checksum = paste0(.data$targetChecksum, .data$comparatorChecksum))
    
    subset <- CohortDiagnostics:::subsetToRequiredCombis(
      combis = combinationsOfPossibleCohortRelationships,
      task = "runCohortRelationship",
      incremental = TRUE,
      recordKeepingFile = paste0(exportFile, "recordKeeping")
    ) %>% dplyr::tibble()
    
    ### subset should not have the combinations in record keeping file
    shouldBeDfOfZeroRows <- subset %>%
      dplyr::inner_join(
        recordKeepingFileData %>%
          dplyr::select(.data$cohortId,
                        .data$comparatorId) %>%
          dplyr::rename(
            targetCohortId = .data$cohortId,
            comparatorCohortId = .data$comparatorId
          ),
        by = c("targetCohortId", "comparatorCohortId")
      )
    
    testthat::expect_equal(
      object = nrow(shouldBeDfOfZeroRows),
      expected = 0,
      info = "Looks like subset and record keeping file did not match."
    )
    
    
    ## running again by adding cohort 2, to previously run 1 and 10
    CohortDiagnostics:::executeCohortRelationshipDiagnostics(
      connection = connectionCohortRelationship,
      databaseId = "testDataSourceName",
      exportFolder = exportFolder,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = tableName,
      tempEmulationSchema = NULL,
      cohortDefinitionSet = cohortDefinitionSet %>%
        dplyr::filter(.data$cohortId %in% c(1, 10, 2)),
      temporalCovariateSettings = list(
        temporalStartDays = c(-365, -30),
        temporalEndDays = c(-31, -1)
      ),
      minCellCount = 0,
      recordKeepingFile = paste0(exportFile, "recordKeeping"),
      incremental = TRUE,
      batchSize = 2
    )
    
    recordKeepingFileData2 <-
      readr::read_csv(file = paste0(exportFile, "recordKeeping"),
                      col_types = readr::cols())
    # record keeping file should have 6 combinations - for 3 cohorts
    testthat::expect_equal(object = nrow(recordKeepingFileData2),
                           expected = 3 * 2 * 1)
    
    #record keeping file should have 4 additional combinations
    testthat::expect_equal(
      object = recordKeepingFileData2 %>%
        dplyr::anti_join(
          recordKeepingFileData %>%
            dplyr::select(.data$cohortId,
                          .data$comparatorId),
          by = c("cohortId", "comparatorId")
        ) %>%
        nrow(),
      expected = 4
    )

    
    # check what happens for an unrelated cohort combination
    allCohortIds <- cohortDefinitionSet  %>%
      dplyr::filter(.data$cohortId %in% c(2, 20)) %>%
      dplyr::select(.data$cohortId, .data$checksum) %>%
      dplyr::rename(targetCohortId = .data$cohortId,
                    targetChecksum = .data$checksum) %>%
      dplyr::distinct()
    
    combinationsOfPossibleCohortRelationships <- allCohortIds %>%
      tidyr::crossing(
        allCohortIds %>%
          dplyr::rename(
            comparatorCohortId = .data$targetCohortId,
            comparatorChecksum = .data$targetChecksum
          )
      ) %>%
      dplyr::filter(.data$targetCohortId != .data$comparatorCohortId) %>%
      dplyr::arrange(.data$targetCohortId, .data$comparatorCohortId) %>%
      dplyr::mutate(checksum = paste0(.data$targetChecksum, .data$comparatorChecksum))
    
    subset <- CohortDiagnostics:::subsetToRequiredCombis(
      combis = combinationsOfPossibleCohortRelationships,
      task = "runCohortRelationship",
      incremental = TRUE,
      recordKeepingFile = paste0(exportFile, "recordKeeping")
    ) %>% dplyr::tibble()
    
    ### subset should be two rows in subsets that are not in record keeping file
    shouldBeTwoRows <- subset %>%
      dplyr::anti_join(
        recordKeepingFileData2 %>%
          dplyr::select(.data$cohortId,
                        .data$comparatorId) %>%
          dplyr::rename(
            targetCohortId = .data$cohortId,
            comparatorCohortId = .data$comparatorId
          ),
        by = c("targetCohortId", "comparatorCohortId")
      )
    
    testthat::expect_equal(
      object = nrow(shouldBeTwoRows),
      expected = 2,
      info = "Looks like subset and record keeping file did not match, Two new cohorts should have run."
    )
    
    
  })
})





test_that("Testing cohort relationship logic - incremental FALSE", {
  skip_if(skipCdmTests, "cdm settings not configured")

  # manually create cohort table and load to table
  # for the logic to work - there has to be some overlap of the comparator cohort over target cohort
  # note - we will not be testing offset in this test. it is expected to work as it is a simple substraction

  temporalStartDays <- c(0)
  temporalEndDays <- c(0)

  targetCohort <- dplyr::tibble(
    cohortDefinitionId = c(1),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1900-01-15")),
    cohortEndDate = c(as.Date("1900-01-31"))
  ) # target cohort always one row

  comparatorCohort <- # all records here overlap with targetCohort
    dplyr::tibble(
      cohortDefinitionId = c(10, 10, 10),
      subjectId = c(1, 1, 1),
      cohortStartDate = c(
        as.Date("1900-01-01"), # starts before target cohort start
        as.Date("1900-01-22"), # starts during target cohort period and ends during target cohort period
        as.Date("1900-01-31")
      ),
      cohortEndDate = c(
        as.Date("1900-01-20"),
        as.Date("1900-01-29"),
        as.Date("1900-01-31")
      )
    )

  cohort <- dplyr::bind_rows(targetCohort, comparatorCohort)

  connectionCohortRelationship <-
    DatabaseConnector::connect(connectionDetails)

  # to do - with incremental = FALSE
  with_dbc_connection(connectionCohortRelationship, {
    sysTime <- as.numeric(Sys.time()) * 100000
    tableName <- paste0("cr", sysTime)
    observationTableName <- paste0("op", sysTime)

    DatabaseConnector::insertTable(
      connection = connectionCohortRelationship,
      databaseSchema = cohortDatabaseSchema,
      tableName = tableName,
      data = cohort,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = FALSE,
      camelCaseToSnakeCase = TRUE,
      progressBar = FALSE
    )

    cohortRelationship <- runCohortRelationshipDiagnostics(
      connection = connectionCohortRelationship,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = tableName,
      targetCohortIds = c(1),
      comparatorCohortIds = c(10),
      relationshipDays = dplyr::tibble(
        startDay = temporalStartDays,
        endDay = temporalEndDays
      )
    )

    sqlDrop <-
      "IF OBJECT_ID('@cohort_database_schema.@cohort_relationship_cohort_table', 'U') IS NOT NULL
            DROP TABLE @cohort_database_schema.@cohort_relationship_cohort_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connectionCohortRelationship,
      sql = sqlDrop,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_relationship_cohort_table = tableName,
      profile = FALSE,
      progressBar = FALSE
    )

    cohortRelationshipT1C10 <- cohortRelationship %>%
      dplyr::filter(.data$cohortId == 1) %>%
      dplyr::filter(.data$comparatorCohortId == 10)

    testthat::expect_equal(
      object = cohortRelationshipT1C10$subCsBeforeTs,
      expected = 1
    ) # there is one subject in comparator that starts before target

    testthat::expect_equal(
      object = cohortRelationshipT1C10$subCsBeforeTe,
      expected = 1
    ) # there is one subject in comparator that starts before target end

    testthat::expect_equal(
      object = cohortRelationshipT1C10$subCsAfterTs,
      expected = 1
    ) # there is one subject in comparator that starts after target start

    testthat::expect_equal(
      object = cohortRelationshipT1C10$subCsAfterTs,
      expected = 1
    ) # there is one subject in comparator that starts after target start

    testthat::expect_equal(
      object = cohortRelationshipT1C10$subCsOnTe,
      expected = 1
    ) # there is one subject in comparator that starts on target end

    testthat::expect_equal(
      object = cohortRelationshipT1C10$subCsWindowT,
      expected = 1
    ) # there is one subject in comparator that started within the window of Target cohort

    testthat::expect_equal(
      object = cohortRelationshipT1C10$subCeWindowT,
      expected = 1
    ) # there is one subject in comparator that ended within the window of Target cohort
  })
})
