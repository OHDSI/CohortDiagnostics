library(magrittr)

source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")

# shinySettings <- list(connectionDetails = DatabaseConnector::createConnectionDetails(dbms = "postgresql",
#                                              server = "localhost/ohdsi",
#                                              user = "postgres",
#                                              password = Sys.getenv("pwPostgres")),
#                       resultsDatabaseSchema =  "phenotype_library",
#                       vocabularyDatabaseSchema =  "phenotype_library")
# shinySettings <- list(dataFolder = "s:/examplePackageOutput")

# Settings when running on server:
defaultLocalDataFolder <- "data"
defaultLocalDataFile <- "PreMerged.RData"

connectionPool <- NULL
defaultServer <- Sys.getenv("phoebedbServer")
defaultDatabase <- Sys.getenv("phoebedb")
defaultPort <- 5432
defaultUser <- Sys.getenv("phoebedbUser")
defaultPassword <- Sys.getenv("phoebedbPw")
defaultResultsSchema <- Sys.getenv("phoebedbTargetSchema")
defaultVocabularySchema <- Sys.getenv("phoebedbVocabSchema")

defaultDatabaseMode <- FALSE # Use file system if FALSE

defaultCohortBaseUrl <- "https://atlas.ohdsi.org/#/cohortdefinition/"
defaultConceptBaseUrl <- "https://athena.ohdsi.org/search-terms/terms/"

cohortDiagnosticModeDefaultTitle <- "Cohort Diagnostics"
phenotypeLibraryModeDefaultTitle <- "Phenotype Library"

defaultAboutText <- "<h3>Cohort Diagnostics</h3> This Cohort Diagnostics app is currently under development. Do not use"

if (!exists("shinySettings")) {
  writeLines("Using default settings")
  databaseMode <- defaultDatabaseMode & defaultServer != ""
  if (databaseMode) {
    connectionPool <- pool::dbPool(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = "postgresql",
      server = paste(defaultServer, defaultDatabase, sep = "/"),
      port = defaultPort,
      user = defaultUser,
      password = defaultPassword
    )
    resultsDatabaseSchema <- defaultResultsSchema
    vocabularyDatabaseSchema <- defaultVocabularySchema
  } else {
    dataFolder <- defaultLocalDataFolder
  }
  cohortBaseUrl <- defaultCohortBaseUrl
  conceptBaseUrl <- defaultConceptBaseUrl
  if (!is.null(defaultAboutText)) {
    aboutText <- defaultAboutText
  } 
} else {
  writeLines("Using settings provided by user")
  databaseMode <- !is.null(shinySettings$connectionDetails)
  if (databaseMode) {
    connectionDetails <- shinySettings$connectionDetails
    if (is(connectionDetails$server, "function")) {
      connectionPool <- pool::dbPool(drv = DatabaseConnector::DatabaseConnectorDriver(),
                                     dbms = "postgresql",
                                     server = connectionDetails$server(),
                                     port = connectionDetails$port(),
                                     user = connectionDetails$user(),
                                     password = connectionDetails$password(),
                                     connectionString = connectionDetails$connectionString())
    } else {
      # For backwards compatibility with older versions of DatabaseConnector:
      connectionPool <- pool::dbPool(drv = DatabaseConnector::DatabaseConnectorDriver(),
                                     dbms = "postgresql",
                                     server = connectionDetails$server,
                                     port = connectionDetails$port,
                                     user = connectionDetails$user,
                                     password = connectionDetails$password,
                                     connectionString = connectionDetails$connectionString)
    }
    resultsDatabaseSchema <- shinySettings$resultsDatabaseSchema
    vocabularyDatabaseSchema <- shinySettings$vocabularyDatabaseSchema
  } else {
    dataFolder <- shinySettings$dataFolder
  }
  cohortBaseUrl <- shinySettings$cohortBaseUrl
  conceptBaseUrl <- shinySettings$cohortBaseUrl
  if (!is.null(shinySettings$aboutText)) {
    aboutText <- shinySettings$aboutText
  }
}

dataModelSpecifications <- read.csv("resultsDataModelSpecification.csv")
# Cleaning up any tables in memory:
suppressWarnings(rm(list = SqlRender::snakeCaseToCamelCase(dataModelSpecifications$tableName)))

if (databaseMode) {
  
  onStop(function() {
    if (DBI::dbIsValid(connectionPool)) {
      writeLines("Closing database pool")
      pool::poolClose(connectionPool)
    }
  })
  
  resultsTablesOnServer <- tolower(DatabaseConnector::dbListTables(connectionPool, schema = resultsDatabaseSchema))
  
  loadResultsTable <- function(tableName, required = FALSE) {
    if (required || tableName %in% resultsTablesOnServer) {
      tryCatch({
        table <- DatabaseConnector::dbReadTable(connectionPool, 
                                                paste(resultsDatabaseSchema, tableName, sep = "."))
      }, error = function(err) {
        stop("Error reading from ", paste(resultsDatabaseSchema, tableName, sep = "."), ": ", err$message)
      })
      colnames(table) <- SqlRender::snakeCaseToCamelCase(colnames(table))
      if (nrow(table) > 0) {
        assign(SqlRender::snakeCaseToCamelCase(tableName), dplyr::as_tibble(table), envir = .GlobalEnv)
      }
    }
  }
  
  loadResultsTable("database", required = TRUE)
  loadResultsTable("cohort", required = TRUE)
  loadResultsTable("cohort_extra")
  loadResultsTable("phenotype_description")
  loadResultsTable("temporal_time_ref")
  loadResultsTable("concept_sets")
  
  # Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
  isEmpty <- function(tableName) {
    sql <- sprintf("SELECT 1 FROM %s.%s LIMIT 1;", resultsDatabaseSchema, tableName)
    oneRow <- DatabaseConnector::dbGetQuery(connectionPool, sql)
    return(nrow(oneRow) == 0)
  }
  
  for (table in c(dataModelSpecifications$tableName, "recommender_set")) {
    if (table %in% resultsTablesOnServer && 
        !exists(SqlRender::snakeCaseToCamelCase(table)) &&
        !isEmpty(table)) {
      assign(SqlRender::snakeCaseToCamelCase(table), dplyr::tibble())
    }
  }
  
  dataSource <- createDatabaseDataSource(connection = connectionPool,
                                         resultsDatabaseSchema = resultsDatabaseSchema,
                                         vocabularyDatabaseSchema = vocabularyDatabaseSchema)
} else {
  localDataPath <- file.path(dataFolder, defaultLocalDataFile)
  if (!file.exists(localDataPath)) {
    stop(sprintf("Local data file %s does not exist.", localDataPath))
  }
  dataSource <- createFileDataSource(localDataPath, envir = .GlobalEnv)
}

if (exists("cohort")) {
  cohort <- get("cohort") %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::mutate(cohortName = stringr::str_remove(.data$cohortName, "\\[.+?\\] "))
  
  if (exists("phenotypeDescription")) {
    cohort <- cohort %>%
      dplyr::mutate(shortName = paste0("C", .data$cohortId - .data$phenotypeId)) 
  } else {
    cohort <- cohort %>%
      dplyr::mutate(shortName = paste0("C", dplyr::row_number())) 
  }
  cohort <- cohort %>%
    dplyr::mutate(compoundName = paste(shortName, cohortName, sep = ": "))
}

if (exists("database")) {
  database <- get("database") %>% 
    dplyr::filter(!database == 'CPRD')
}

if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- get("temporalTimeRef") %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}

if (exists("covariateRef")) {
  specifications <- readr::read_csv(file = "Table1Specs.csv", 
                                    col_types = readr::cols(),
                                    guess_max = min(1e7))
  prettyAnalysisIds <- specifications$analysisId
}

if (exists("phenotypeDescription")) {
  phenotypeDescription <- phenotypeDescription %>% 
    dplyr::mutate(overview = (stringr::str_match(.data$clinicalDescription, 
                                                 "Overview:(.*?)Presentation:"))[,2] %>%
                    stringr::str_squish() %>% 
                    stringr::str_trim()) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                 pattern = "Overview:", 
                                                                 replacement = "<strong>Overview:</strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                 pattern = "Assessment:", 
                                                                 replacement = "<br/><br/> <strong>Assessment:</strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                 pattern = "Presentation:", 
                                                                 replacement = "<br/><br/> <strong>Presentation: </strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription,
                                                                 pattern = "Plan:",
                                                                 replacement = "<br/><br/> <strong>Plan: </strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription,
                                                                 pattern = "Prognosis:",
                                                                 replacement = "<br/><br/> <strong>Prognosis: </strong>")) %>% 
    dplyr::inner_join(cohort %>%
                        dplyr::group_by(.data$phenotypeId) %>%
                        dplyr::summarize(cohortDefinitions = dplyr::n()) %>%
                        dplyr::ungroup(),
                      by = "phenotypeId")
  searchTerms <- getSearchTerms(dataSource = dataSource, includeDescendants = FALSE) %>% 
    dplyr::group_by(.data$phenotypeId) %>%
    dplyr::summarise(searchTermString = paste(.data$term, collapse = ", ")) %>%
    dplyr::ungroup()
  
  phenotypeDescription <- phenotypeDescription %>%
    dplyr::left_join(searchTerms,
                     by = "phenotypeId")
}
