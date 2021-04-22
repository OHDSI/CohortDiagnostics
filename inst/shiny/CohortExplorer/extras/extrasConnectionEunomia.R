connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
vocabularyDatabaseSchema <- cdmDatabaseSchema
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
databaseId <- "Eunomia"
dbms = 'postgresql'
cohortDefinitionId = 17493

temporaryLocation <- tempdir()

connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)

CohortDiagnostics::instantiateCohortSet(connection = connection, 
                                        cdmDatabaseSchema = cdmDatabaseSchema, 
                                        vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
                                        cohortDatabaseSchema = cohortDatabaseSchema, 
                                        cohortTable = cohortTable, 
                                        generateInclusionStats = TRUE,
                                        packageName = 'SkeletonCohortDiagnosticsStudy',
                                        inclusionStatisticsFolder = file.path(temporaryLocation, 
                                                                              'inclusionStatisticsFolder'))

shinySettings <- list(connection = connection,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortDatabaseSchema = cohortDatabaseSchema,
                      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                      cohortTable = cohortTable,
                      dbms = dbms,
                      cohortDefinitionId = cohortDefinitionId,
                      tempEmulationSchema = NULL,
                      sampleSize = 10)
