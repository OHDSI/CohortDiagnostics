cohortBaseUrl <- "https://atlas.ohdsi.org/#/cohortdefinition/"
conceptBaseUrl <- "https://athena.ohdsi.org/search-terms/terms/"
cohortDiagnosticModeDefaultTitle <- "Cohort Diagnostics"
phenotypeLibraryModeDefaultTitle <- "Phenotype Library"

thresholdCohortSubjects <- 0
thresholdCohortEntries <- 0




#####################
# if (!is.null(Sys.getenv('user')) &&
#     !is.null(Sys.getenv('phenotypeLibraryDbServer'))) {
#   connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
#                                                                   server = paste(Sys.getenv("phenotypeLibraryDbServer"),
#                                                                                  Sys.getenv("phenotypeLibraryDbDatabase"),
#                                                                                  sep = "/"),
#                                                                   port = Sys.getenv("phenotypeLibraryDbPort"),
#                                                                   user = Sys.getenv("phenotypeLibraryDbUser"),
#                                                                   password = Sys.getenv("phenotypeLibraryDbPassword"))
#   connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
#   resultsDatabaseSchema <- Sys.getenv('phenotypeLibraryResultsDbSchema')
#   vocabularyDatabaseSchema <- Sys.getenv('phenotypeLibraryVocabularyDbSchema')
# } else {
#   connection <- NULL
# }
# 
# queryAllData <- function(connection,
#                          databaseSchema, 
#                          tableName) {
#   sql <- "SELECT * FROM @database_schema.@table_name"
#   data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
#                                                      sql = sql,
#                                                      snakeCaseToCamelCase = TRUE,
#                                                      database_schema = databaseSchema,
#                                                      table_name = tableName
#   ) %>% 
#     dplyr::tibble()
#   return(data)
# }
