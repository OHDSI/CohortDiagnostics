library(magrittr)

source("R/Init.R")
source("R/Tables.R")
source("R/Other.R")
source("R/Plots.R")
source("R/Results.R")

connectionPool <- NULL
defaultLocalDataFolder <- "data"
defaultLocalDataFile <- "PreMerged.RData"

if (!exists("shinySettings")) {
  shinySettings <- list(
    connectionDetails = getDbConnectionDetails(),
    resultsDatabaseSchema = getResultsDatabaseSchema(),
    cdmDatabaseSchema = getCdmDatabaseSchema(),
    dataFolder = defaultLocalDataFolder
  )
} else {
  if (is.null(shinySettings$connectionDetails)) {
    shinySettings$connectionDetails <- getDbConnectionDetails()
  }
  if (is.null(shinySettings$dataFolder)) {
    shinySettings$dataFolder <- defaultLocalDatFolder
  }
  if (is.null(shinySettings$resultsDatabaseSchema)) {
    shinySettings$resultsDatabaseSchema <- getResultsDatabaseSchema()
  }
  if (is.null(shinySettings$cdmDatabaseSchema)) {
    shinySettings$cdmDatabaseSchema <- getCdmDatabaseSchema()
  }
}



suppressWarnings(rm(list = resultsGlobalReferenceTables))
suppressWarnings(rm(list = cdmGlobalReferenceTables))


# Cleanup the database connPool if it was created
# (borrowed from https://github.com/ohdsi-studies/Covid19CharacterizationCharybdis/blob/master/inst/shiny/CharybdisResultsExplorer/global.R)
onStop(function() {
  if (!is.null(connectionPool)) {
    if (DBI::dbIsValid(connectionPool)) {
      writeLines("Closing database pool")
      pool::poolClose(connectionPool)
    }
  }
})


if (is.null(shinySettings$connectionDetails)) {
  warning("No database connection details. Looking for local data.")
  if (is.null(shinySettings$dataFolder)) {
    stop("No mechanism to load data.")
  }
  
  localDataPath <- file.path(shinySettings$dataFolder, defaultLocalDataFile)
  if (!file.exists(localDataPath)) {
    stop(sprintf("Local data file %s does not exist.", localDataPath))
  }
    
  loadGlobalDataFromLocal(localDataPath)
} else {
  
  connectionPool <- pool::dbPool(
    drv = DatabaseConnector::DatabaseConnectorDriver(),
    dbms = shinySettings$connectionDetails$dbms,
    server = shinySettings$connectionDetails$server,
    port = shinySettings$connectionDetails$port,
    user = shinySettings$connectionDetails$user,
    password = shinySettings$connectionDetails$password
  )
  loadGlobalDataFromDatabase(connection = connectionPool,
                             resultsDatabaseSchema = shinySettings$resultsDatabaseSchema,
                             cdmDatabaseSchema = shinySettings$cdmDatabaseSchema,
                             verbose = TRUE)
  
  instantiateEmptyTableObjects(connection = connectionPool,
                               resultsDatabaseSchema = shinySettings$resultsDatabaseSchema,
                               cdmDatabaseSchema = shinySettings$cdmDatabaseSchema)
}



if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- temporalTimeRef %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}



# 
# if (is.null(connection)) {
#   if (!exists("shinySettings")) {
#     if (file.exists("data")) {
#       shinySettings <- list(dataFolder = "data")
#     } else {
#       shinySettings <- list(dataFolder = "S:/examplePackageOutput")
#     }
#     dataFolder <- shinySettings$dataFolder
#     if (file.exists(file.path(dataFolder, "PreMerged.RData"))) {
#       writeLines(paste0("Using merged data detected in folder '", dataFolder, "'"))
#       load(file.path(dataFolder, "PreMerged.RData"))
#     } else {
#       stop("No premerged file found")
#     }
#   }
# } else {
#   writeLines(paste0("Retrieving some tables from databse "))
#   allTables <- DatabaseConnector::getTableNames(connection = connection,
#                                                 databaseSchema = resultsDatabaseSchema)
#   globalTables <- c('analysis_ref', 'temporal_time_ref', 'cohort', 'cohort_count',
#                     'concept_sets', 'database', 'phenotype_description')
#   for (i in (1:length(globalTables))) {
#     assign(SqlRender::snakeCaseToCamelCase(globalTables[[i]]), 
#            queryAllData(connection = connection,
#                         databaseSchema = resultsDatabaseSchema,
#                         tableName = globalTables[[i]]))
#   }
#   blankTables <- setdiff(x = allTables, y = globalTables)
#   for (i in (1:length(blankTables))) {
#     assign(SqlRender::snakeCaseToCamelCase(blankTables[[i]]),
#            tidyr::tibble())
#   }
# }
