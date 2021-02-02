library(magrittr)
appVersion <- "2.1.0"

source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")
source("R/ConceptRecommender.R")
source("R/DataPulls.R")
source("R/Connections.R")
source("R/HelperFunctions.R")
source("R/ModifyDataSource.R")

# Settings when running on server:
assign(x = "defaultLocalDataFolder", value = "data", envir = .GlobalEnv)
assign(x = "defaultLocalDataFile", value = "PreMerged.RData", envir = .GlobalEnv)
assign(x = "isValidConnection", value = FALSE, envir = .GlobalEnv)

assign(x = "defaultDatabaseMode", value = TRUE, envir = .GlobalEnv) # Set to FALSE if using file system.
assign(x = "dbms", value = "postgresql", envir = .GlobalEnv)
assign(x = "port", value = 5432, envir = .GlobalEnv)

# default app titles and text
assign(x = "cohortDiagnosticModeDefaultTitle", value = "Cohort Diagnostics", envir = .GlobalEnv)
assign(x = "phenotypeLibraryModeDefaultTitle", value = "Phenotype Library", envir = .GlobalEnv)
source("html/defaultAboutTextPhenotypeLibrary.txt")

# Cleaning up any tables in memory:
dataModelSpecifications <-
  readr::read_csv(
    file = "resultsDataModelSpecification.csv",
    col_types = readr::cols(),
    guess_max = min(1e7)
  )
suppressWarnings(rm(list = snakeCaseToCamelCase(dataModelSpecifications$tableName)))

# connection information
if (!exists("shinySettings")) {
  # shinySettings object is from CohortDiagnostics::launchDiagnosticsExplorer()
  writeLines("Using default settings -- attempting to connect to OHDSI phenotype library")
  assign(x = "usingUserProvidedSettings", value = FALSE, envir = .GlobalEnv)
  if (Sys.getenv("phoebedbUser") != '') {
    assign("username", Sys.getenv("phoebedbUser"), envir = .GlobalEnv)
  }
  if (Sys.getenv("phoebedbPw") != '') {
    assign("password", Sys.getenv("phoebedbPw"), envir = .GlobalEnv)
  }
  if (Sys.getenv("phoebedbServer") != '') {
    assign("server", Sys.getenv("phoebedbServer"), envir = .GlobalEnv)
  }
  if (Sys.getenv("phoebedb") != '') {
    assign("database", Sys.getenv("phoebedb"), envir = .GlobalEnv)
  }
  if (all((Sys.getenv("phoebedbServer") != ''),
          (Sys.getenv("phoebedb") != ''))) {
    assign("server", paste(
      Sys.getenv("phoebedbServer"),
      Sys.getenv("phoebedb"),
      sep = "/"
    ),
    envir = .GlobalEnv)
  }
  if (Sys.getenv("phoebedbVocabSchema") != '') {
    assign("vocabularyDatabaseSchema",
           Sys.getenv("phoebedbVocabSchema"),
           envir = .GlobalEnv)
  }
  if (Sys.getenv("phoebedbTargetSchema") != '') {
    assign("resultsDatabaseSchema",
           Sys.getenv("phoebedbTargetSchema"),
           envir = .GlobalEnv)
  }
  
  if (server != "" &&
      database != "" &&
      username != "" &&
      password != "" &&
      port != "") {
    # writeLines(text = "Checking Connection parameters.")
    connectionIsValid <- try(isConnectionValid(
      dbms = dbms,
      server = server,
      port = port,
      username = username,
      password = password
    ))
    if (connectionIsValid) {
      assign(x = "isValidConnection",
             value = TRUE,
             envir = .GlobalEnv)
      connectionDetails <-
        DatabaseConnector::createConnectionDetails(
          dbms = dbms,
          server = server,
          port = port,
          user = username,
          password = password
        )
      connection <-
        DatabaseConnector::connect(connectionDetails = connectionDetails)
      # writeLines(text = "Database Connector Connection.")
      connectionPool <- NULL
      # writeLines(text = "Connecting to Pool.")
      connectionPool <- pool::dbPool(
        drv = DatabaseConnector::DatabaseConnectorDriver(),
        dbms = dbms,
        server = paste(server, database, sep = "/"),
        port = port,
        user = username,
        password = password
      )
      writeLines(text = "Connected.")
    }
  }
  if (!is.null(x = defaultAboutTextPhenotypeLibrary)) {
    aboutText <- defaultAboutTextPhenotypeLibrary
  }
  userNotification <-
    paste0("Cohort Diagnostics app (version ", appVersion, ")")
} else {
  assign(x = "usingUserProvidedSettings", value = TRUE, envir = .GlobalEnv)
  databaseMode <- !is.null(x = shinySettings$connectionDetails)
  if (!is.null(x = shinySettings$aboutText)) {
    aboutText <- shinySettings$aboutText
  } else {
    aboutText <- ''
  }
  if (databaseMode) {
    writeLines(text = "Using user provided settings - connecting to database in dbms mode.")
    userNotification <- paste0("Connected to database.")
    connectionDetails <- shinySettings$connectionDetails
    if (is(object = connectionDetails$server, class2 = "function")) {
      drv <- DatabaseConnector::DatabaseConnectorDriver()
      dbms <- connectionDetails$dbms()
      server <- connectionDetails$server()
      port <- connectionDetails$port()
      user <- connectionDetails$user()
      password <- connectionDetails$password()
      connectionString <- connectionDetails$connectionString()
    } else {
      # For backwards compatibility with older versions of DatabaseConnector:
      drv <- DatabaseConnector::DatabaseConnectorDriver()
      dbms <- connectionDetails$dbms
      server <- connectionDetails$server
      port <- connectionDetails$port
      user <- connectionDetails$user
      password <- connectionDetails$password
      connectionString <- connectionDetails$connectionString
    }
    connectionIsValid <- isConnectionValid(
      dbms = dbms,
      server = server,
      port = port,
      username = username,
      password = password
    )
    if (connectionIsValid) {
      assign(x = "isValidConnection",
             value = TRUE,
             envir = .GlobalEnv)
      connectionDetails <-
        DatabaseConnector::createConnectionDetails(
          dbms = dbms,
          server = server,
          port = port,
          user = username,
          password = password
        )
      connection <-
        DatabaseConnector::connect(connectionDetails = connectionDetails)
      connectionPool <- NULL
      connectionPool <- pool::dbPool(
        drv = DatabaseConnector::DatabaseConnectorDriver(),
        dbms = database,
        server = paste(server, database, sep = "/"),
        port = port,
        user = user,
        password = password
      )
      writeLines(text = "Connected.")
      if (!is.null(x = shinySettings$resultsDatabaseSchema)) {
        writeLines(text = "No results database schema provided.")
      } else {
        resultsDatabaseSchema <- shinySettings$resultsDatabaseSchema
      }
      if (!is.null(x = shinySettings$vocabularyDatabaseSchema)) {
        writeLines(text = "No results database schema provided.")
      } else {
        vocabularyDatabaseSchema <- shinySettings$vocabularyDatabaseSchema
      }
    } else {
      writeLines(text = "User provided connection parameters are not valid.")
    }
  } else {
    writeLines(text = "Using user provided settings - running on local mode. Looking for premerged file.")
    userNotification <- paste0("Using premerged file.")
    if (!is.null(x = shinySettings$dataFolder)) {
      dataFolder <- shinySettings$dataFolder
    } else {
      writeLines(text = "No data folder provided.User provided settings are not valid.")
      dataFolder <- NULL
    }
    if (!is.null(x = shinySettings$dataFile)) {
      writeLines(text = "No data file provided. User provided settings are not valid.")
      dataFile <- shinySettings$dataFile
    } else {
      dataFile <- NULL
    }
  }
}

# Cleanup connection when the application stops
shiny::onStop(function() {
  if (isValidConnection) {
    writeLines(text = "Closing database connections")
    if (DBI::dbIsValid(dbObj = connectionPool)) {
      pool::poolClose(pool = connectionPool)
    }
    if (DBI::dbIsValid(dbObj = connection)) {
      DatabaseConnector::disconnect(connection = connection)
    }
  }
})


if (isValidConnection) {
  resultsTablesOnServer <-
    tolower(x = DatabaseConnector::dbListTables(conn = connectionPool,
                                                schema = resultsDatabaseSchema))
  
  # the code section below instantiates set of tables in R memory.
  # some tables are 'dummy' tables.
  loadRequiredTables(tableName = "database",
                     databaseSChema = resultsDatabaseSchema,
                     required = TRUE)
  loadRequiredTables(tableName = "cohort",
                     databaseSChema = resultsDatabaseSchema,
                     required = TRUE)
  loadRequiredTables(tableName = "cohort_extra", databaseSChema = resultsDatabaseSchema)
  loadRequiredTables(tableName = "phenotype_description", databaseSChema = resultsDatabaseSchema)
  loadRequiredTables(tableName = "temporal_time_ref", databaseSChema = resultsDatabaseSchema)
  loadRequiredTables(tableName = "concept_sets", databaseSChema = resultsDatabaseSchema)
  loadRequiredTables(tableName = "analysis_ref", databaseSChema = resultsDatabaseSchema)
  loadRequiredTables(tableName = "temporal_analysis_ref", databaseSChema = resultsDatabaseSchema)
  
  for (table in c(dataModelSpecifications$tableName, "recommender_set")) {
    if (table %in% resultsTablesOnServer &&
        !exists(x = snakeCaseToCamelCase(string = table)) &&
        !isEmpty(
          connection = connectionPool,
          tableName = table,
          resultsDatabaseSchema = resultsDatabaseSchema
        )) {
      assign(
        x = snakeCaseToCamelCase(table),
        value = dplyr::tibble(),
        envir = .GlobalEnv
      )
    }
  }
  dataSource <-
    createDatabaseDataSource(
      connection = connectionPool,
      resultsDatabaseSchema = resultsDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema
    )
} else {
  localDataPath <- file.path(dataFolder, dataFile)
  if (!file.exists(localDataPath)) {
    stop(sprintf("Local data file %s does not exist.", localDataPath))
  }
  dataSource <-
    createFileDataSource(premergedDataFile = localDataPath, envir = .GlobalEnv)
}


# create memory variables based on
if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- get("temporalTimeRef") %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>%
    dplyr::arrange(.data$timeId)
  assign(x = "temporalCovariateChoices", value = temporalCovariateChoices, envir = .GlobalEnv)
}
if (exists("covariateRef")) {
  specifications <- readr::read_csv(
    file = "Table1Specs.csv",
    col_types = readr::cols(),
    guess_max = min(1e7)
  )
  assign(x = "prettyAnalysisIds",
         value = specifications$analysisId,
         envir = .GlobalEnv)
}


referentConceptIds <- c(0)
# modify tables in memory - process cohort table.
if (exists("cohort")) {
  # this table is required for app to work.
  cohort <- get("cohort") %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::mutate(cohortName = stringr::str_remove(.data$cohortName, "\\[.+?\\] "))
  
  if ('metadata' %in% colnames(cohort)) {
    cohortMetaData <- list()
    for (i in 1:nrow(cohort)) {
      x <- RJSONIO::fromJSON(cohort[i, ]$metadata)
      for (j in 1:length(x)) {
        if (!any(is.null(x[[j]]), is.na(x[[j]]))) {
          x[[j]] <- stringr::str_split(string = x[[j]], pattern = ";")[[1]]
        }
      }
      x <- dplyr::bind_rows(x)
      x$cohort_id <- cohort[i, ]$cohortId
      x$phenotype_id <- cohort[i, ]$phenotypeId
      cohortMetaData[[i]] <- x
    }
    cohortMetaData <- dplyr::bind_rows(cohortMetaData) %>%
      readr::type_convert(col_types = readr::cols())
    if ('referent_concept_id' %in% colnames(cohortMetaData)) {
      referentConceptIds <-
        c(referentConceptIds,
          cohortMetaData$referent_concept_id) %>% unique()
    }
    colnames(cohortMetaData) <-
      snakeCaseToCamelCase(colnames(cohortMetaData))
  }
} else {
  writeLines("Cohort table not found")
}


if (exists("phenotypeDescription")) {
  # this table is optional.
  phenotypeDescription <- phenotypeDescription %>%
    dplyr::mutate(overview = (
      stringr::str_match(.data$clinicalDescription,
                         "Overview:(.*?)Presentation:")
    )[, 2] %>%
      stringr::str_squish() %>%
      stringr::str_trim()) %>%
    dplyr::mutate(
      clinicalDescription = stringr::str_replace_all(
        string = .data$clinicalDescription,
        pattern = "Overview:",
        replacement = "<strong>Overview:</strong>"
      )
    ) %>%
    dplyr::mutate(
      clinicalDescription = stringr::str_replace_all(
        string = .data$clinicalDescription,
        pattern = "Assessment:",
        replacement = "<br/><br/> <strong>Assessment:</strong>"
      )
    ) %>%
    dplyr::mutate(
      clinicalDescription = stringr::str_replace_all(
        string = .data$clinicalDescription,
        pattern = "Presentation:",
        replacement = "<br/><br/> <strong>Presentation: </strong>"
      )
    ) %>%
    dplyr::mutate(
      clinicalDescription = stringr::str_replace_all(
        string = .data$clinicalDescription,
        pattern = "Plan:",
        replacement = "<br/><br/> <strong>Plan: </strong>"
      )
    ) %>%
    dplyr::mutate(
      clinicalDescription = stringr::str_replace_all(
        string = .data$clinicalDescription,
        pattern = "Prognosis:",
        replacement = "<br/><br/> <strong>Prognosis: </strong>"
      )
    ) %>%
    dplyr::inner_join(
      cohort %>%
        dplyr::group_by(.data$phenotypeId) %>%
        dplyr::summarize(cohortDefinitions = dplyr::n()) %>%
        dplyr::ungroup(),
      by = "phenotypeId"
    ) %>% 
    dplyr::mutate(referentConceptId = .data$phenotypeId/1000) %>% 
    dplyr::select(.data$phenotypeId, .data$phenotypeName,
                  .data$clinicalDescription, .data$overview,
                  .data$cohortDefinitions, .data$referentConceptId)
  
  referentConceptIds <-
    c(referentConceptIds,
      phenotypeDescription$referentConceptId) %>% unique()
}

if (isValidConnection) {
  referentConceptIdsDataFrame <-
    queryRenderedSqlFromDatabase(
      connection = connection,
      sql = SqlRender::render(
        sql = SqlRender::readSql("sql/ConceptSynonymNamesForListOfConceptIds.sql"),
        vocabulary_database_schema = vocabularyDatabaseSchema,
        concept_id_list = referentConceptIds
      )
    ) %>%
    dplyr::arrange(.data$conceptId)
} else {
  referentConceptIdsDataFrame <-
    dplyr::tibble(conceptId = 0, conceptSynonymName = 'No matching concept')
}

referentConceptIdsSearchTerms <- referentConceptIdsDataFrame %>%
  dplyr::group_by(.data$conceptId) %>%
  dplyr::summarise(conceptNameSearchTerms = toString(.data$conceptSynonymName)) %>%
  dplyr::ungroup()

# pubmedQueryString <- tidyr::replace_na(data = cohortMetaData$pmid %>% unique(),
#                                        replace = 0) %>%
#   paste(collapse = '[UID] OR ')
# pubmedIds <- easyPubMed::get_pubmed_ids(pubmed_query_string = pubmedQueryString)
# pubmedXmlData <- easyPubMed::fetch_pubmed_data(pubmed_id_list = pubmedIds)


if (exists('cohortMetaData')) {
  if ('referentConceptId' %in% colnames(cohortMetaData)) {
    cohortReferentConceptSearchTerms <- cohortMetaData %>%
      dplyr::select(.data$cohortId, .data$referentConceptId) %>% 
      dplyr::left_join(referentConceptIdsSearchTerms,
                       by = c("referentConceptId" = "conceptId")) %>%
      dplyr::group_by(.data$cohortId) %>% 
      dplyr::mutate(referentConceptId = paste0(as.character(.data$referentConceptId), collapse = ", "),
                    referentConceptIdsSearchTerms = paste0(.data$conceptNameSearchTerms, collapse = ", ")) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(referentConceptIdsSearchTerms = paste(referentConceptId,referentConceptIdsSearchTerms)) %>%
      dplyr::select(-.data$referentConceptId, - .data$conceptNameSearchTerms)
    
    cohort <- cohort %>% 
      dplyr::left_join(y = cohortReferentConceptSearchTerms, by = c('cohortId'))
    
    remove(cohortReferentConceptSearchTerms)
  }
  if ('cohortType' %in% colnames(cohortMetaData)) {
    cohortType <- cohortMetaData %>%
      dplyr::select(.data$cohortId, .data$cohortType) %>% 
      dplyr::group_by(.data$cohortId) %>% 
      dplyr::mutate(cohortType = paste0(.data$cohortType, collapse = ", ")) %>% 
      dplyr::ungroup()
    
    cohort <- cohort %>% 
      dplyr::left_join(y = cohortType, by = c('cohortId'))
    
    remove(cohortType)
  }
}
remove(cohortMetaData)

if (exists('phenotypeDescription')) {
  if ('referentConceptId' %in% colnames(phenotypeDescription)) {
    phenotypeDescription <- phenotypeDescription %>%
      dplyr::left_join(referentConceptIdsSearchTerms,
                       by = c('referentConceptId' = 'conceptId')) %>%
      dplyr::select(-.data$referentConceptId) %>%
      dplyr::mutate(
        referentConceptIdsSearchTerms = paste0(.data$conceptNameSearchTerms, collapse = ",")
      )
  }
}