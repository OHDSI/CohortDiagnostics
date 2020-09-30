library(magrittr)

source("R/Init.R")
source("R/Tables.R")
source("R/Other.R")
source("R/Plots.R")
source("R/Results.R")

databaseConnection <- NULL
defaultLocalDataFolder <- "data"
defaultLocalDataFile <- "PreMerged.RData"
resultsDatabaseSchema <- NULL
cdmDatabaseSchema <- NULL

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


if (is.null(shinySettings$connectionDetails)) {
  warning("No database connection details. Looking for local data.")
  if (is.null(shinySettings$dataFolder)) {
    stop("No mechanism to load data.")
  } else {
    localDataPath <- shinySettings$dataFolder
    if (!file.exists(localDataPath)) {
      stop(sprintf("Local data path %s does not exist.", localDataPath))
    } else {
      localDataPath <- file.path(localDataPath, defaultLocalDataFile)
      if (!file.exists(localDataPath)) {
        stop(sprintf("Local data file %s does not exist.", localDataPath))
      }
      
      loadGlobalDataFromLocal(localDataPath)
    }
  }
} else {
  databaseConnection <- DatabaseConnector::connect(connectionDetails = shinySettings$connectionDetails)
  loadGlobalDataFromDatabase(databaseConnection,
                             resultsDatabaseSchema = shinySettings$resultsDatabaseSchema,
                             cdmDatabaseSchema = shinySettings$cdmDatabaseSchema,
                             verbose = TRUE)
}



if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- temporalTimeRef %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}
