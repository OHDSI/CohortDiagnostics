library(magrittr)

#Source scripts ----
source("R/Shared.R")
source("R/StartUpScripts.R")
source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")

#Set values to NULL
connectionPool <- NULL

appTitleHeader <- "Cohort Diagnostics"

#Load default environment variables----
defaultLocalDataFolder <- "data"
defaultLocalDataFile <- "PreMerged.RData"

# OHDSI Shiny DB
defaultServer <- Sys.getenv("shinydbServer")
defaultDatabase <- Sys.getenv("shinydbDatabase")
defaultPort <- 5432
defaultUser <- Sys.getenv("shinydbUser")
defaultPassword <- Sys.getenv("shinydbPw")
defaultResultsSchema <- 'cdSkeletoncohortdiagnosticsstudy2'
defaultVocabularySchema <- defaultResultsSchema
alternateVocabularySchema <- c('vocabulary')

# OHDSI Phenotype DB
# defaultServer <- Sys.getenv("phenotypeLibraryServer")
# defaultDatabase <- Sys.getenv("phenotypeLibrarydb")
# defaultPort <- 5432
# defaultUser <- Sys.getenv("phenotypeLibrarydbUser")
# defaultPassword <- Sys.getenv("phenotypeLibrarydbPw")
# defaultResultsSchema <- Sys.getenv("phenotypeLibrarydbTargetSchema")
# defaultVocabularySchema <- Sys.getenv("phenotypeLibrarydbVocabSchema")
# alternateVocabularySchema <- c('vocabulary')

#Mode
defaultDatabaseMode <- FALSE # Use file system if FALSE

#Configuration variables ----
showIncidenceRate <- TRUE
showTimeSeries <- TRUE
showTimeDistribution <- TRUE
showIndexEventBreakdown <- TRUE
showVisitContext <- TRUE
showCharacterization <- TRUE
showTemporalCharacterization <- TRUE
filterTemporalChoicesToPrimaryOptions <- TRUE

showConceptBrowser <- TRUE  #on selected conceptId - show concept browser  (applied for cohort, index event breakdown, characterization tab)
# (applies to cohort, indexEventBreakdown, characterization, temporalCharacterization, compareCharacterization, temporalCompareCharacterization)
showConceptSetComparison <- TRUE #given two concept set - show difference in resolved concepts
allCohortsToBeCompared <- TRUE # in cohort tab, allow comparator cohort selection, and comparator cohort
showNotes <- TRUE # in cohort count tab, show notes for count
show3DTempolarCharecterizationPlot <- FALSE

# Foot note ----
appInformationText <- "V 2.2"
appInformationText <-
  paste0(
    "Powered by OHDSI Cohort Diagnostics application - ",
    appInformationText,
    ". This app is working in"
  )

#Launch settings ----
if (!exists("shinySettings")) {
  writeLines("Using default settings")
  databaseMode <- defaultDatabaseMode & defaultServer != ""
  if (databaseMode) {
    appInformationText <- paste0(appInformationText, " database")
    connectionPool <- pool::dbPool(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = "postgresql",
      server = paste(defaultServer, defaultDatabase, sep = "/"),
      port = defaultPort,
      user = defaultUser,
      password = defaultPassword
    )
    resultsDatabaseSchema <- defaultResultsSchema
  } else {
    dataFolder <- defaultLocalDataFolder
    appInformationText <- paste0(appInformationText, " local file")
  }
  vocabularyDatabaseSchemas <-
    setdiff(x = c(defaultVocabularySchema, alternateVocabularySchema),
            y = defaultResultsSchema) %>%
    unique() %>%
    sort()
} else {
  writeLines("Using settings provided by user")
  databaseMode <- !is.null(shinySettings$connectionDetails)
  if (databaseMode) {
    appInformationText <- paste0(appInformationText, " database")
    connectionDetails <- shinySettings$connectionDetails
    if (is(connectionDetails$server, "function")) {
      connectionPool <-
        pool::dbPool(
          drv = DatabaseConnector::DatabaseConnectorDriver(),
          dbms = "postgresql",
          server = connectionDetails$server(),
          port = connectionDetails$port(),
          user = connectionDetails$user(),
          password = connectionDetails$password(),
          connectionString = connectionDetails$connectionString()
        )
    } else {
      appInformationText <- paste0(appInformationText, " local file")
      # For backwards compatibility with older versions of DatabaseConnector:
      connectionPool <-
        pool::dbPool(
          drv = DatabaseConnector::DatabaseConnectorDriver(),
          dbms = "postgresql",
          server = connectionDetails$server,
          port = connectionDetails$port,
          user = connectionDetails$user,
          password = connectionDetails$password,
          connectionString = connectionDetails$connectionString
        )
    }
    resultsDatabaseSchema <- shinySettings$resultsDatabaseSchema
    vocabularyDatabaseSchemas <-
      shinySettings$vocabularyDatabaseSchemas
  } else {
    dataFolder <- shinySettings$dataFolder
  }
}

## Launch information ----
appInformationText <- paste0(
  appInformationText,
  " mode. Application was last initiated on ",
  lubridate::now(tzone = "EST"),
  " EST. Cohort Diagnostics website is at https://ohdsi.github.io/CohortDiagnostics/"
)

#Expected tables based on data model specifications
dataModelSpecifications <-
  getResultsDataModelSpecifications()
dataModelSpecifications21 <-
  getResultsDataModelSpecifications(versionNumber = 2.1)

#Clean up shadows ----
suppressWarnings(rm(
  list = SqlRender::snakeCaseToCamelCase(dataModelSpecifications$tableName %>% unique())
))


#Loading data ----
if (databaseMode) {
  # close connection to database on app stop
  onStop(function() {
    if (DBI::dbIsValid(connectionPool)) {
      writeLines("Closing database pool")
      pool::poolClose(connectionPool)
    }
  })
  
  # tables observed in database
  resultsTablesOnServer <-
    tolower(DatabaseConnector::dbListTables(connectionPool,
                                            schema = resultsDatabaseSchema))
  
  #!!!!!!!!! write logic to infer if the data model is 2.1 or 2.2 here - for backward compatibility
  ####load tables into R memory ----
  tablesToLoadRequired <- c("cohort", "cohort_count", "database")
  tablesToLoad <-
    c(
      "analysis_ref",
      "concept_sets",
      "concept_class",
      "concept_resolved",
      # "covariate_ref",
      "domain",
      "relationship",
      "temporal_time_ref",
      "temporal_analysis_ref",
      # "temporal_covariate_ref",
      "vocabulary"
    )
  for (i in (1:length(tablesToLoadRequired))) {
    loadResultsTable(
      tableName = tablesToLoadRequired[[i]],
      resultsTablesOnServer = resultsTablesOnServer,
      required = TRUE
    )
  }
  
  for (i in (1:length(tablesToLoad))) {
    loadResultsTable(
      tableName = tablesToLoad[[i]],
      resultsTablesOnServer = resultsTablesOnServer,
      required = FALSE
    )
  }
  
  # compare expected to observed tables
  for (table in c(dataModelSpecifications$tableName %>% unique())) {
    #, "recommender_set"
    if (table %in% resultsTablesOnServer &&
        !exists(SqlRender::snakeCaseToCamelCase(table)) &&
        !isEmpty(table)) {
      #if table is empty in remote database, then this condition will be FALSE.
      assign(SqlRender::snakeCaseToCamelCase(table),
             dplyr::tibble())
    }
  }
  dataSource <-
    createDatabaseDataSource(
      connection = connectionPool,
      resultsDatabaseSchema = resultsDatabaseSchema,
      vocabularyDatabaseSchema = resultsDatabaseSchema
    )
} else {
  localDataPath <- file.path(dataFolder, defaultLocalDataFile)
  if (!file.exists(localDataPath)) {
    stop(sprintf("Local data file %s does not exist.", localDataPath))
  }
  dataSource <-
    createFileDataSource(localDataPath, envir = .GlobalEnv)
}


#Adding enhancements to the objects, which are already loaded in R memory----
if (exists("database") &&  doesObjectHaveData(database)) {
  if (nrow(database) > 0 &&
      "vocabularyVersion" %in% colnames(database)) {
    database <- database %>%
      dplyr::mutate(
        databaseIdWithVocabularyVersion = paste0(databaseId, " (", .data$vocabularyVersion, ")")
      )
  }
}

if (all(exists("cohort"),
        doesObjectHaveData(cohort))) {
  
  #enhance cohortCount table to have all cohortId and databaseId combinations
  cohortCount <- tidyr::crossing(database %>% dplyr::select(.data$databaseId) %>% dplyr::distinct(),
                                 cohort %>% dplyr::select(.data$cohortId) %>% dplyr::distinct()) %>% 
    dplyr::left_join(cohortCount, by = c("databaseId", "cohortId")) %>% 
    dplyr::arrange(.data$databaseId, .data$cohortId)
  
  # cohort is required and is always loaded into R memory
  cohort <- cohort %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::mutate(shortName = paste0("C", .data$cohortId)) %>%
    dplyr::mutate(compoundName = paste0(
      .data$shortName,
      ": ",
      .data$cohortName
    ))
}

if (all(exists("conceptSets"),
        doesObjectHaveData(conceptSets))) {
  # cohort is required and is always loaded into R memory
  conceptSets <- conceptSets %>%
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId,
                                      .data$shortName)) %>%
    dplyr::mutate(compoundName = paste0(
      .data$shortName,
      "-",
      .data$conceptSetId,
      ": ",
      .data$conceptSetName
    )) %>% 
    dplyr::select(-.data$shortName) %>% 
    dplyr::arrange(.data$cohortId, .data$conceptSetId)
} else if (all(exists("cohort"),
               doesObjectHaveData(cohort$json))) {
  conceptSets <- list()
  k <- 0
  for (i in (1:nrow(cohort))) {
    conceptSetDetails <-
      getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = cohort[i,]$json %>%
                                                 RJSONIO::fromJSON(digits = 23))
    for (j in (1:nrow(conceptSetDetails$conceptSetExpression))) {
      k <- k + 1
      df <- dplyr::tibble(
        cohortId = cohort[i, ]$cohortId,
        conceptSetId = conceptSetDetails$conceptSetExpression[j,]$id,
        conceptSetName = conceptSetDetails$conceptSetExpression[j,]$name,
        conceptSetSql = "Please run concept set diagnostics to get SQL.",
        conceptSetExpression = conceptSetDetails$conceptSetExpression[j, ]$expression %>% RJSONIO::toJSON(digits = 23,
                                                                                                          pretty = TRUE)
      )
      conceptSets[[k]] <- df
    }
  }
  conceptSets <- dplyr::bind_rows(conceptSets) %>%
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId,
                                      .data$shortName)) %>%
    dplyr::mutate(compoundName = paste0(
      .data$shortName,
      "-",
      .data$conceptSetId,
      ": ",
      .data$conceptSetName
    )) %>% 
    dplyr::select(-.data$shortName) %>% 
    dplyr::arrange(.data$cohortId, .data$conceptSetId)
}

if (all(exists("database"),
        doesObjectHaveData(database))) {
  # cohort is required and is always loaded into R memory
  database <- database %>%
    dplyr::arrange(.data$databaseId) %>%
    dplyr::mutate(shortName = paste0("D", dplyr::row_number())) %>%
    dplyr::mutate(compoundName = paste0(
      .data$shortName,
      ": ",
      .data$databaseName,
      "(",
      .data$databaseId,
      ")"
    ))
}

#enhancement 
if (all(exists("temporalTimeRef"),
        doesObjectHaveData(temporalTimeRef))) {
  if (all(
    nrow(temporalTimeRef) > 0
  )) {
    temporalCovariateChoices <- temporalTimeRef %>%
      dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
      dplyr::select(.data$timeId, .data$choices) %>%
      dplyr::arrange(.data$timeId)
    
    if (filterTemporalChoicesToPrimaryOptions) {
      temporalCovariateChoices <- temporalCovariateChoices %>%
        dplyr::filter(
          stringr::str_detect(string = .data$choices,
                              pattern = 'Start -365 to end -31|Start -30 to end -1|Start 0 to end 0|Start 1 to end 30|Start 31 to end 365')
        )
    }
  }
}

conceptSetRelationshipName <- relationship %>% 
  dplyr::arrange(.data$relationshipName) %>% 
  dplyr::pull(.data$relationshipName) %>% unique()


#enhancement objects based on the control variable
table1Specs <- readr::read_csv(
  file = "Table1Specs.csv",
  col_types = readr::cols(),
  guess_max = min(1e7)
)
prettyAnalysisIds <- table1Specs$analysisId


if (!showCharacterization) {
  if (exists("covariateValue")) {rm("covariateValue")}
  if (exists("covariateValueDist")) {rm("covariateValueDist")}
  if (exists("analysisRef")) {rm("analysisRef")}
  if (exists("covariateRef")) {rm("covariateRef")}
}
if (!showTemporalCharacterization) {
  if (exists("covariateValue")) {rm("temporalCovariateValue")}
  if (exists("covariateValueDist")) {rm("temporalCovariateValueDist")}
  if (exists("analysisRef")) {rm("temporalAnalysisRef")}
  if (exists("covariateRef")) {rm("temporalCovariateRef")}
}

#!!!!!!!!!!!!reduce code lines here
# disable tabs based on user preference or control variable ----
if (!showIncidenceRate) {
  if (exists("showIncidenceRate")) {
    rm("incidenceRate")
  }
}

if (!showTimeSeries) {
  if (exists("timeSeries")) {
    rm("timeSeries")
  }
}

if (!showTimeDistribution) {
  if (exists("timeDistribution")) {
    rm("timeDistribution")
  }
}

if (!showIndexEventBreakdown) {
  if (exists("indexEventBreakdown")) {
    rm("indexEventBreakdown")
  }
}

if (!showVisitContext) {
  if (exists("visitContext")) {
    rm("visitContext")
  }
}

#!!!!!! incomplete logic
# if (!showOverlap) {
#   if (exists("visitContext")) {
#     rm("visitContext")
#   }
# }


#Extras -----
# other objects in memory ----
sourcesOfVocabularyTables <-
  getSourcesOfVocabularyTables(dataSource = dataSource,
                               database = database)
