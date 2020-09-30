library(magrittr)

source("R/Tables.R")
source("R/Other.R")
source("R/Plots.R")
source("R/Results.R")
source("R/DB.R")


#TODO: CDM data schema vs results data schema
#      (i.e., atm, assumes all tables in resultsDatabaseSchema)
databaseConnection <- NULL
defaultLocalDataFolder <- "data"
defaultLocalDataFile <- "PreMerged.RData"
resultsDatabaseSchema <- "diagnostics"

if (!exists("shinySettings")) {
  shinySettings <- list(
    connectionDetails = getDbConnectionDetails(),
    dataFolder = defaultLocalDataFolder
  )
} else {
  if (is.null(shinySettings$connectionDetails)) {
    shinySettings$connectionDetails <- getDbConnectionDetails()
  }
  if (is.null(shinySettings$dataFolder)) {
    shinySettings$dataFolder <- defaultLocalDatFolder
  }
}



suppressWarnings(
  rm(list = globalReferenceTables)
  # rm(
  #   "analysisRef",
  #   "temporalAnalysisRef",
  #   "temporalTimeRef",
  #   "covariateRef",
  #   "temporarlCovariateRef",
  #   "concept",
  #   "vocabulary",
  #   "domain",
  #   "conceptAncestor",
  #   "conceptRelationship",
  #   "cohort",
  #   "cohortCount",
  #   "cohortOverlap",
  #   "conceptSets",
  #   "database",
  #   "incidenceRate",
  #   "includedSourceConcept",
  #   "inclusionRuleStats",
  #   "indexEventBreakdown",
  #   "orphanConcept",
  #   "timeDistribution",
  #   "visitContext"
  # )
)


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
                             databaseSchema = resultsDatabaseSchema,
                             verbose = TRUE)
}



if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- temporalTimeRef %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}
