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
  
  # Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
  isEmpty <-
    function(connection,
             resultsDatabaseSchema,
             tableName) {
      sql <-
        sprintf("SELECT 1 FROM %s.%s LIMIT 1;",
                resultsDatabaseSchema,
                tableName)
      oneRow <-
        DatabaseConnector::dbGetQuery(conn = connection, sql) %>%
        dplyr::tibble()
      return(nrow(oneRow) == 0)
    }
  
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
  
  if ('phenotypeId' %in% colnames(cohort)) {
    referentConceptIds <- unique(cohort$phenotypeId / 1000)
  }
  
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
    dplyr::mutate(
      searchTerms = paste(
        .data$phenotypeId,
        .data$phenotypeName,
        .data$clinicalDescription,
        sep = ", "
      )
    ) %>%
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
    )
  
  if ('metadata' %in% colnames(phenotypeDescription)) {
    phenotypeDescriptionMetaData <- list()
    for (i in 1:nrow(phenotypeDescription)) {
      x <- RJSONIO::fromJSON(phenotypeDescription[i, ]$metadata)
      for (j in 1:length(x)) {
        if (!any(is.null(x[[j]]), is.na(x[[j]]))) {
          x[[j]] <- stringr::str_split(string = x[[j]], pattern = ";")[[1]]
        }
      }
      x <- dplyr::bind_rows(x)
      x$phenotype_id <- cohort[i, ]$phenotypeId
      phenotypeDescriptionMetaData[[i]] <- x
    }
    phenotypeDescriptionMetaData <-
      dplyr::bind_rows(phenotypeDescriptionMetaData) %>%
      readr::type_convert(col_types = readr::cols())
    if ('referent_concept_id' %in% colnames(phenotypeDescriptionMetaData)) {
      referentConceptIds <-
        c(referentConceptIds,
          phenotypeDescriptionMetaData$referent_concept_id) %>% unique()
    }
  }
  colnames(phenotypeDescriptionMetaData) <-
    snakeCaseToCamelCase(colnames(phenotypeDescriptionMetaData))
  # get concept name and concept synonyms for all referent concepts to memory
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
  cohortMetaData$searchTerms <- ""
  if ('referentConceptId' %in% colnames(cohortMetaData)) {
    cohortMetaData <- cohortMetaData %>%
      dplyr::left_join(referentConceptIdsSearchTerms,
                       by = c("referentConceptId" = "conceptId")) %>%
      dplyr::select(-.data$referentConceptId) %>%
      dplyr::mutate(searchTerms = paste(searchTerms, conceptNameSearchTerms, sep = ", ")) %>%
      dplyr::select(-.data$conceptNameSearchTerms)
  }
  cohortMetaData <- cohortMetaData %>%
    dplyr::left_join(
      cohort %>%
        dplyr::mutate(
          cohortSearchString = paste(
            .data$phenotypeId,
            .data$cohortId,
            .data$cohortName,
            .data$logicDescription,
            sep = ", "
          )
        ) %>%
        dplyr::select(.data$cohortId, .data$cohortName, .data$cohortSearchString),
      by = "cohortId"
    ) %>%
    dplyr::mutate(searchTerms = paste(searchTerms, cohortSearchString, cohortType, sep = ", ")) %>%
    dplyr::select(.data$cohortId, .data$searchTerms) %>%
    dplyr::distinct()
  cohort <- cohort %>%
    dplyr::left_join(cohortMetaData, by = "cohortId")
} else {
  cohort <- cohort %>%
    dplyr::mutate(searchTerms = paste(.data$cohortName, .data$logicDescription, sep = ", "))
}
remove(cohortMetaData)

if (exists('phenotypeDescriptionMetaData')) {
  if ('referentConceptId' %in% colnames(phenotypeDescriptionMetaData)) {
    phenotypeDescriptionMetaData <- phenotypeDescriptionMetaData %>%
      dplyr::left_join(referentConceptIdsSearchTerms,
                       by = c('referentConceptId' = 'conceptId')) %>%
      dplyr::rename(phenotypeReferentConceptsSearchTerms = conceptNameSearchTerms) %>%
      dplyr::select(-.data$referentConceptId) %>%
      dplyr::group_by(.data$phenotypeId) %>%
      dplyr::summarise(
        phenotypeReferentConceptsSearchTerms = toString(.data$phenotypeReferentConceptsSearchTerms)
      ) %>%
      dplyr::ungroup()
    phenotypeDescription <- phenotypeDescription %>%
      dplyr::left_join(phenotypeDescriptionMetaData, by = 'phenotypeId') %>%
      dplyr::mutate(searchTerms = paste(searchTerms, phenotypeReferentConceptsSearchTerms, sep = ", ")) %>%
      dplyr::select(-.data$phenotypeReferentConceptsSearchTerms)
  }
}
remove(phenotypeDescriptionMetaData)

if (exists('phenotypeDescription')) {
  cohort <- cohort %>%
    dplyr::left_join(
      phenotypeDescription %>%
        dplyr::rename(phenotypeSearchTerms = searchTerms) %>%
        dplyr::select(.data$phenotypeId, .data$phenotypeSearchTerms),
      by = "phenotypeId"
    ) %>%
    dplyr::mutate(searchTerms = paste(searchTerms, phenotypeSearchTerms, sep = ", ")) %>%
    dplyr::select(-.data$phenotypeSearchTerms) %>%
    dplyr::group_by(.data$phenotypeId) %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::left_join(dplyr::select(phenotypeDescription, phenotypeId, phenotypeName),
                     by = "phenotypeId") %>%
    dplyr::mutate(shortName = paste0(
      "C",
      dplyr::row_number(),
      " (Phenotype: ",
      .data$phenotypeName,
      ") "
    )) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(compoundName = paste(shortName, cohortName, sep = ": "))
} else {
  cohort <- cohort %>%
    dplyr::group_by(.data$phenotypeId) %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::mutate(shortName = paste0("C", dplyr::row_number())) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(compoundName = paste(shortName, cohortName, sep = ": "))
}

options(DT.options = list(
  pageLength = 10,
  lengthMenu = c(5, 10, 15, 20, 100, 500, 1000),
  lengthChange = TRUE,
  searching = TRUE,
  ordering = TRUE,
  scrollX = TRUE,
  ordering = TRUE,
  paging = TRUE,
  info = TRUE,
  searchHighlight = TRUE,
  # search = list(regex = TRUE, caseInsensitive = FALSE),
  stateSave = TRUE,
  dom = 'Bfrtip', # for buttons
  buttons = c('copy', 'csv', 'excel', 'pdf', 'print', 'colvis'),
  colReorder = TRUE,
  realtime = FALSE, # for col reorder
  # fixedColumns = list(leftColumns = 1),
  # fixedHeader = TRUE,
  # processing = TRUE,
  autoWidth = TRUE
))

assign(x = "defaultDataTableFilter",
       value =
         list(
           position = 'top',
           clear = TRUE,
           plain = FALSE
         ))


#
# # getSearchTerms might not be useful in future.
# if (exists("phenotypeDescription")) {
#   searchTerms <- getSearchTerms(dataSource = dataSource,
#                                 includeDescendants = FALSE) %>%
#     dplyr::group_by(.data$phenotypeId) %>%
#     dplyr::summarise(searchTermString = paste(.data$term, collapse = ", ")) %>%
#     dplyr::ungroup()
#
#   phenotypeDescription <- phenotypeDescription %>%
#     dplyr::left_join(searchTerms,
#                      by = "phenotypeId")
# }