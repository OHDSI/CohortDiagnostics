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

# Other Phenotype DB

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
showCohortOverlap <- TRUE
showPlotSpikes <- TRUE
# filterTemporalChoicesToPrimaryOptions <- TRUE
spinnerType = 8

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
                                      .data$shortName),
                      by = "cohortId") %>%
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
                                      .data$shortName),
                      by = "cohortId") %>%
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
    dplyr::mutate(id = dplyr::row_number()) %>% 
    dplyr::mutate(shortName = paste0("D", .data$id)) %>%
    dplyr::mutate(compoundName = paste0(
      .data$shortName,
      ": ",
      .data$databaseId
    )) %>% 
    dplyr::arrange(.data$id)
}

#enhancement 
if (all(exists("temporalTimeRef"),
        doesObjectHaveData(temporalTimeRef))) {
  if (all(
    nrow(temporalTimeRef) > 0
  )) {
    temporalCovariateChoices <- temporalTimeRef %>%
      dplyr::arrange(.data$startDay, .data$endDay) %>%
      dplyr::filter(!(abs(.data$endDay - .data$startDay) == 30) |
                      .data$endDay == 0) %>%
      dplyr::mutate(
        temporalName = dplyr::case_when(
          .data$endDay == 0 & .data$startDay == -30 ~ "Baseline (Short Term)",
          .data$endDay == 0 & .data$startDay == -180 ~ "Baseline (Medium Term)",
          .data$endDay == 0 & .data$startDay == -365 ~ "Baseline (Long Term)",
          .data$endDay == 0 & .data$startDay == -9999 ~ "Baseline (Any-time prior)",
          .data$endDay == -31 & .data$startDay == -365 ~ "Temporal (-31d to -365d)",
          .data$endDay == -1 & .data$startDay == -30 ~ "Temporal (-1d to -30d)",
          .data$endDay == 0 & .data$startDay == 0 ~ "Temporal (0d to 0d)",
          .data$endDay == 30 & .data$startDay == 1 ~ "Temporal (1d to 30d)",
          .data$endDay == 365 & .data$startDay == 31 ~ "Temporal (31d to 365d)"
        )
      ) %>%
      dplyr::mutate(
        sequence = dplyr::case_when(
          .data$endDay == 0 & .data$startDay == -30 ~ 2,
          .data$endDay == 0 & .data$startDay == -180 ~ 3,
          .data$endDay == 0 & .data$startDay == -365 ~ 4,
          .data$endDay == 0 & .data$startDay == -9999 ~ 1,
          .data$endDay == -31 & .data$startDay == -365 ~ 5,
          .data$endDay == -1 & .data$startDay == -30 ~ 6,
          .data$endDay == 0 & .data$startDay == 0 ~ 7,
          .data$endDay == 30 & .data$startDay == 1 ~ 8,
          .data$endDay == 365 & .data$startDay == 31 ~ 9
        )
      ) %>%
      dplyr::arrange(.data$sequence) %>% 
      dplyr::mutate(choices = .data$temporalName) %>%
      dplyr::select(.data$startDay, .data$endDay, .data$choices)
    
    # if (filterTemporalChoicesToPrimaryOptions) {
    #   temporalCovariateChoices <- temporalCovariateChoices %>%
    #     dplyr::filter(
    #       stringr::str_detect(string = .data$choices,
    #                           pattern = 'Start -365 to end -31|Start -30 to end -1|Start 0 to end 0|Start 1 to end 30|Start 31 to end 365')
    #     )
    # }
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
#!!!!!!!!!!! need to update prettyAnalysisId


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

prettyTable1Specifications <- readr::read_csv(
  file = "Table1SpecsLong.csv",
  col_types = readr::cols(),
  guess_max = min(1e7),
  lazy = FALSE
)

colorReference <-
  readr::read_csv(
    file = 'colorReference.csv',
    col_types = readr::cols(),
    guess_max = min(1e7)
  )

#Extras -----
# other objects in memory ----
sourcesOfVocabularyTables <-
  getSourcesOfVocabularyTables(dataSource = dataSource,
                               database = database)



# 
# 
# temporalCovariateChoices <- dplyr::tibble()
# newTimeId <- dplyr::bind_rows(
#   startDay = c(-365, -30, 0, 1, 31),
#   endDay = c(-31, -1, 0, 30, 365)
# ) %>% 
#   dplyr::mutate(timeId2 = (dplyr::row_number()) - 6)
# #enhancement
# if (all(exists("analysisRef"),
#         doesObjectHaveData(analysisRef))) {
#   if (all(nrow(analysisRef) > 0)) {
#     temporalCovariateChoices <- dplyr::bind_rows(
#       temporalCovariateChoices,
#       analysisRef %>%
#         dplyr::filter(!is.na(.data$startDay),!is.na(.data$endDay)) %>%
#         dplyr::select(.data$startDay, .data$endDay) %>%
#         dplyr::distinct() %>%
#         dplyr::mutate(choices = paste0(
#           "Start ", .data$startDay, " to end ", .data$endDay
#         ))
#     ) %>%
#       dplyr::select(.data$startDay, .data$endDay, .data$choices) %>%
#       dplyr::mutate(timeId = dplyr::row_number()) %>% 
#       dplyr::left_join(newTimeId, 
#                        by = c("startDay", "endDay")) %>%
#       dplyr::mutate(timeId = dplyr::case_when(!is.na(.data$timeId2) ~ .data$timeId2,
#                                               TRUE ~ as.double(.data$timeId))) %>% 
#       dplyr::select(-.data$timeId2) %>% 
#       dplyr::arrange(.data$timeId)
#     
#     if (filterTemporalChoicesToPrimaryOptions) {
#       temporalCovariateChoices <- temporalCovariateChoices %>%
#         dplyr::filter(
#           stringr::str_detect(string = .data$choices,
#                               pattern = 'Start -365 to end -31|Start -30 to end -1|Start 0 to end 0|Start 1 to end 30|Start 31 to end 365')
#         )
#     }
#   }
# }
# 
# if (all(exists("temporalTimeRef"),
#         doesObjectHaveData(temporalTimeRef))) {
#   if (all(nrow(temporalTimeRef) > 0)) {
#     temporalCovariateChoices <-
#       dplyr::bind_rows(temporalCovariateChoices,
#                        temporalTimeRef %>%
#                          dplyr::mutate(choices = paste0(
#                            "Start ", .data$startDay, " to end ", .data$endDay
#                          ))) %>%
#       dplyr::distinct() %>%
#       dplyr::select(-.data$timeId) %>%
#       dplyr::select(.data$startDay, .data$endDay) %>%
#       dplyr::mutate(timeId = dplyr::row_number()) %>%
#       dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
#       dplyr::select(.data$startDay, .data$endDay, .data$choices) %>%
#       dplyr::mutate(timeId = dplyr::row_number()) %>% 
#       dplyr::left_join(newTimeId, 
#                        by = c("startDay", "endDay")) %>%
#       dplyr::mutate(timeId = dplyr::case_when(!is.na(.data$timeId2) ~ .data$timeId2,
#                                               TRUE ~ as.double(.data$timeId))) %>% 
#       dplyr::select(-.data$timeId2) %>% 
#       dplyr::arrange(.data$timeId)
#     
#     if (filterTemporalChoicesToPrimaryOptions) {
#       temporalCovariateChoices <- temporalCovariateChoices %>%
#         dplyr::filter(
#           stringr::str_detect(string = .data$choices,
#                               pattern = 'Start -365 to end -31|Start -30 to end -1|Start 0 to end 0|Start 1 to end 30|Start 31 to end 365')
#         )
#     }
#   }
# }
# 

