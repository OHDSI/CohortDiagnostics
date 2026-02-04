# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.availableTables <- function(connectionHandler, schema) {
  if (connectionHandler$dbms() == "postgresql") {
    sql <- "SELECT table_name FROM information_schema.tables where table_schema = '@schema'"
    tables <- connectionHandler$queryDb(sql, schema = schema) |>
      dplyr::pull("tableName") |>
      tolower()
    
  } else if (connectionHandler$dbms() != "sqlite") {
    tables <- DatabaseConnector::getTableNames(connectionHandler$getConnection(),
                                               databaseSchema = schema) |>
      tolower()
  } else {
    tables <- DatabaseConnector::getTableNames(connectionHandler$getConnection()) |>
      tolower()
  }
  return(tables)
}


# NOTE: here it would be nice to use dbplyr tables - this would allow lazy loading of resources
# however, renaming the columns causes an error and its not obvious how it could be resolved
loadResultsTable <- function(dataSource, tableName, required = FALSE, cdTablePrefix = "", databaseTablePrefix = "") {
  selectTableName <- paste0(ifelse(!tolower(tableName) == 'database_meta_data', cdTablePrefix,databaseTablePrefix), tableName)
  resultsTablesOnServer <-
    tolower(.availableTables(dataSource$connectionHandler, dataSource$schema))
  
  if (required || selectTableName %in% resultsTablesOnServer) {
    if (tableIsEmpty(dataSource, selectTableName)) {
      return(data.frame())
    }
    
    tryCatch(
      {
        
        if(tolower(tableName) == 'database_meta_data'){
          table <- dataSource$connectionHandler$queryDb(
            "SELECT *, cdm_source_name as database_name FROM @schema.@table",
            schema = dataSource$schema,
            table = selectTableName
          )
        } else{
          table <- dataSource$connectionHandler$queryDb(
            "SELECT * FROM @schema.@table",
            schema = dataSource$schema,
            table = selectTableName
          )
        }
      },
      error = function(err) {
        stop(
          "Error reading from ",
          paste(dataSource$resultsDatabaseSchema, selectTableName, sep = "."),
          ": ",
          err$message
        )
      }
    )
    
    return(table)
  }
  
  return(data.frame())
}

# Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
tableIsEmpty <- function(dataSource, tableName) {
  sql <- "SELECT * FROM @schema.@table LIMIT 1"
  row <- data.frame()
  tryCatch({
    row <- dataSource$connectionHandler$queryDb(
      sql,
      schema = dataSource$schema,
      table = tableName
    )
    
  }, error = function(...) {
    message("Table not found: ", tableName)
  })
  
  return(nrow(row) == 0)
}

postgresEnabledReports <- function(connectionHandler, schema, tbls) {
  
  sql <- "
    select c.relname as table_name
  from pg_class c
  inner join pg_namespace n on n.oid = c.relnamespace
  where c.relkind = 'r'
        and n.nspname not in ('information_schema','pg_catalog')
        and c.reltuples != 0
        and n.nspname = '@schema'
        "
  
  return(connectionHandler$queryDb(sql, schema = schema) %>% dplyr::pull("tableName"))
}

#' Get enable cd reports from available data
#' @param dataSource     Cohort diagnostics data source
#' @family CohortDiagnostics
#' @export
getEnabledCdReports <- function(dataSource) {
  
  if (dataSource$connectionHandler$dbms() == "postgresql") {
    tbls <- dataSource$dataModelSpecifications$tableName %>% unique()
    possible <- paste0(dataSource$cdTablePrefix, tbls)
    available <- postgresEnabledReports(dataSource$connectionHandler, dataSource$schema)
    enabledReports <- c(tbls[possible %in% available], "cohort", "database") %>%
      unique() %>%
      SqlRender::snakeCaseToCamelCase()
    return(enabledReports)
  }
  
  enabledReports <- c()
  resultsTables <- .availableTables(dataSource$connectionHandler, schema = dataSource$schema)
  
  for (table in dataSource$dataModelSpecifications$tableName %>% unique()) {
    if (dataSource$prefixTable(table) %in% resultsTables) {
      if (!tableIsEmpty(dataSource, dataSource$prefixTable(table))) {
        enabledReports <- c(enabledReports, SqlRender::snakeCaseToCamelCase(table))
      }
    }
  }
  enabledReports <- c(enabledReports, "cohort", "database")
  
  return(enabledReports)
}

#' Create a CD data source from a database
#'
#' @description use this to create an interface to cohort diagnostics results data
#' NOTE: I think this would make a good R6 class for other objects in this package so you could query them outside of
#' a shiny app. E.g. if you wanted to make a custom R markdown template
#'
#' @param connectionHandler An instance of a ResultModelManager::connectionHander - manages a connection to a database.
#' @param resultDatabaseSettings a list containing the result schema and prefixes
#' @param dataModelSpecificationsPath The path to a file containing specifications for the data model used by the database.
#' @param displayProgress display a progress messaage (can only be used inside a shiny reactive context)
#' @param dataMigrationsRef The path to a file listing all migrations for the data model that should have been applied
#' @return An object of class `CdDataSource`.
#' @family CohortDiagnostics
#' @export
createCdDatabaseDataSource <- function(
    connectionHandler,
    resultDatabaseSettings,
    dataModelSpecificationsPath = system.file("cohort-diagnostics-ref",
                                              "resultsDataModelSpecification.csv",
                                              package = utils::packageName()),
    dataMigrationsRef = system.file("cohort-diagnostics-ref",
                                    "migrations.csv",
                                    package = utils::packageName()),
    displayProgress = FALSE
) {
  
  checkmate::assertR6(connectionHandler, "ConnectionHandler")
  checkmate::assertString(resultDatabaseSettings$schema)
  checkmate::assertString(resultDatabaseSettings$vocabularyDatabaseSchema, null.ok = TRUE)
  checkmate::assertString(resultDatabaseSettings$cdTablePrefix, null.ok = TRUE)
  checkmate::assertString(resultDatabaseSettings$cgTable, null.ok = TRUE)
  checkmate::assertString(resultDatabaseSettings$databaseTable, null.ok = TRUE)
  checkmate::assertString(resultDatabaseSettings$databaseTablePrefix, null.ok = TRUE)
  checkmate::assertFileExists(dataModelSpecificationsPath)
  checkmate::assertFileExists(dataMigrationsRef)
  
  if (is.null(resultDatabaseSettings$vocabularyDatabaseSchema)) {
    resultDatabaseSettings$vocabularyDatabaseSchema <- resultDatabaseSettings$schema
  }
  if (is.null(resultDatabaseSettings$cdTablePrefix)) {
    resultDatabaseSettings$cdTablePrefix <- ""
  }
  if (is.null(resultDatabaseSettings$cgTable)) {
    resultDatabaseSettings$cgTable <- "cohort"
  }
  if (is.null(resultDatabaseSettings$cgTablePrefix)) {
    resultDatabaseSettings$cgTablePrefix <- resultDatabaseSettings$cdTablePrefix
  }
  if (is.null(resultDatabaseSettings$databaseTable)) {
    resultDatabaseSettings$databaseTable <- "database"
  }
  if (is.null(resultDatabaseSettings$databaseTablePrefix)) {
    resultDatabaseSettings$databaseTablePrefix <- resultDatabaseSettings$cdTablePrefix
  }
  
  if (displayProgress) {
    shiny::setProgress(value = 0.05, message = "Getting settings")
  }
  migrations <- data.frame()
  # Check existence of migrations table - display warnings if not present or if it is out of date
  tryCatch({
    migrations <- connectionHandler$queryDb("SELECT * FROM @schema.@cd_table_prefixmigration",
                                            snakeCaseToCamelCase = TRUE,
                                            schema = resultDatabaseSettings$schema,
                                            cd_table_prefix = resultDatabaseSettings$cdTablePrefix)
  }, error = function(...) {
    warning("CohortDiagnostics schema does not contain migrations table. Schema was likely created incorrectly")
    if (displayProgress) {
      shiny::showNotification(paste("CohortDiagnostics data model does not have migrations table. Schema was likely created incorrectly"),
                              type = "error")
    }
  })
  
  dataMigrationsExpected <- utils::read.csv(dataMigrationsRef)
  for (m in dataMigrationsExpected$migrationFile) {
    if (!m %in% migrations$migrationFile) {
      warning(paste("CohortDiagnostics data migration", m, "not executed!"))
      if (displayProgress) {
        shiny::showNotification(paste("CohortDiagnostics data migration", m, "not executed!"), type = "warning")
      }
    }
  }
  
  modelSpec <- utils::read.csv(dataModelSpecificationsPath)
  colnames(modelSpec) <- SqlRender::snakeCaseToCamelCase(colnames(modelSpec))
  
  dataSource <- list(
    connectionHandler = connectionHandler,
    schema = resultDatabaseSettings$schema,
    vocabularyDatabaseSchema = resultDatabaseSettings$vocabularyDatabaseSchema,
    dbms = connectionHandler$dbms(),
    resultsTablesOnServer = .availableTables(connectionHandler, resultDatabaseSettings$schema),
    cdTablePrefix = resultDatabaseSettings$cdTablePrefix,
    prefixTable = function(tableName) {
      if(tableName != resultDatabaseSettings$databaseTable){
        return(paste0(resultDatabaseSettings$cdTablePrefix, tableName))
      } else{
        return(paste0(resultDatabaseSettings$databaseTablePrefix, tableName))
      }
      },
    prefixVocabTable = function(tableName) {
      # don't prexfix table if we us a dedicated vocabulary schema
      if (resultDatabaseSettings$vocabularyDatabaseSchema == resultDatabaseSettings$schema)
        return(paste0(resultDatabaseSettings$cdTablePrefix, tableName))
      
      return(tableName)
    },
    cgTable = resultDatabaseSettings$cgTable,
    cgTablePrefix = resultDatabaseSettings$cgTablePrefix,
    useCgTable = FALSE,
    databaseTable = resultDatabaseSettings$databaseTable,#"database",
    databaseTablePrefix = resultDatabaseSettings$databaseTablePrefix,#"cd_",
    dataModelSpecifications = modelSpec,
    migrations = migrations
  )
  
  if (displayProgress)
    shiny::setProgress(value = 0.05, message = "Getting enabled reports")
  
  dataSource$enabledReports <- getEnabledCdReports(dataSource)
  
  if (displayProgress)
    shiny::setProgress(value = 0.1, message = "Getting database information")
  dataSource$dbTable <- getDatabaseTable(dataSource)
  
  if (displayProgress)
    shiny::setProgress(value = 0.2, message = "Getting cohorts")
  
  
  dataSource$cohortTableName <- paste0(dataSource$cdTablePrefix, "cohort")
  
  dataSource$cohortTable <- getCohortTable(dataSource)
  
  if (displayProgress)
    shiny::setProgress(value = 0.6, message = "Getting concept sets")
  
  dataSource$conceptSets <- loadResultsTable(dataSource, "concept_sets", cdTablePrefix = dataSource$cdTablePrefix)
  
  if (displayProgress)
    shiny::setProgress(value = 0.7, message = "Getting counts")
  
  dataSource$cohortCountTable <- loadResultsTable(dataSource, "cohort_count", required = TRUE, cdTablePrefix = dataSource$cdTablePrefix)
  
  dataSource$enabledReports <- dataSource$enabledReports
  
  if (displayProgress)
    shiny::setProgress(value = 0.7, message = "Getting Temporal References")
  
  dataSource$temporalAnalysisRef <- loadResultsTable(dataSource, "temporal_analysis_ref", cdTablePrefix = dataSource$cdTablePrefix)
  
  dataSource$temporalChoices <- getResultsTemporalTimeRef(dataSource = dataSource)
  
  if (hasData(dataSource$temporalChoices)) {
    dataSource$temporalCharacterizationTimeIdChoices <- dataSource$temporalChoices %>%
      dplyr::arrange(.data$sequence)
    
    dataSource$characterizationTimeIdChoices <- dataSource$temporalChoices %>%
      dplyr::filter(.data$isTemporal == 0) %>%
      dplyr::filter(.data$primaryTimeId == 1) %>%
      dplyr::arrange(.data$sequence)
  }
  
  if (!is.null(dataSource$temporalAnalysisRef)) {
    dataSource$temporalAnalysisRef <- dplyr::bind_rows(
      dataSource$temporalAnalysisRef,
      dplyr::tibble(
        analysisId = c(-201, -301),
        analysisName = c("CohortEraStart", "CohortEraOverlap"),
        domainId = "Cohort",
        isBinary = "Y",
        missingMeansZero = "Y"
      )
    )
    
    dataSource$domainIdOptions <- dataSource$temporalAnalysisRef %>%
      dplyr::select("domainId") %>%
      dplyr::pull("domainId") %>%
      unique() %>%
      sort()
    
    dataSource$analysisNameOptions <- dataSource$temporalAnalysisRef %>%
      dplyr::select("analysisName") %>%
      dplyr::pull("analysisName") %>%
      unique() %>%
      sort()
  }
  
  class(dataSource) <- "CdDataSource"
  return(dataSource)
}

getDatabaseTable <- function(dataSource) {
  databaseTable <- loadResultsTable(dataSource, dataSource$prefixTable(dataSource$databaseTable), required = TRUE)
  
  if (nrow(databaseTable) > 0 &
      "vocabularyVersion" %in% colnames(databaseTable)) {
    databaseTable <- databaseTable %>%
      dplyr::mutate(
        databaseIdWithVocabularyVersion = paste0(.data$databaseId, " (", .data$vocabularyVersion, ")")
      )
  }
  
  databaseTable
}

# SO much of the app requires this table in memory - it would be much better to re-write queries to not need it!
getCohortTable <- function(dataSource) {
  cohortTable <- dataSource$connectionHandler$queryDb(
    "SELECT cohort_id, cohort_name FROM @schema.@cd_table_prefixcohort",
    schema = dataSource$schema,
    cd_table_prefix = dataSource$cdTablePrefix
  )
  
  cohortTable <- cohortTable %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::mutate(shortName = paste0("C", .data$cohortId)) %>%
    dplyr::mutate(compoundName = paste0(.data$shortName, ": ", .data$cohortName))
  
  cohortTable
}

getResultsTemporalTimeRef <- function(dataSource) {
  sql <- "SELECT *
            FROM @schema.@table_name;"
  temporalTimeRef <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      table_name = dataSource$prefixTable("temporal_time_ref")
    )
  
  if (nrow(temporalTimeRef) == 0) {
    return(NULL)
  }
  
  temporalChoices <- temporalTimeRef %>%
    dplyr::mutate(temporalChoices = paste0("T (", .data$startDay, "d to ", .data$endDay, "d)")) %>%
    dplyr::arrange(.data$startDay, .data$endDay) %>%
    dplyr::select(
      "timeId",
      "startDay",
      "endDay",
      "temporalChoices"
    ) %>%
    dplyr::mutate(primaryTimeId = dplyr::if_else(
      condition = (
        (.data$startDay == -365 & .data$endDay == -31) |
          (.data$startDay == -30 & .data$endDay == -1) |
          (.data$startDay == 0 & .data$endDay == 0) |
          (.data$startDay == 1 & .data$endDay == 30) |
          (.data$startDay == 31 & .data$endDay == 365) |
          (.data$startDay == -365 & .data$endDay == 0) |
          (.data$startDay == -30 & .data$endDay == 0)
      ),
      true = 1,
      false = 0
    )) %>%
    dplyr::mutate(isTemporal = dplyr::if_else(
      condition = (
        (.data$endDay == 0 & .data$startDay == -30) |
          (.data$endDay == 0 & .data$startDay == -180) |
          (.data$endDay == 0 & .data$startDay == -365) |
          (.data$endDay == 0 & .data$startDay == -9999)
      ),
      true = 0,
      false = 1
    )) %>%
    dplyr::arrange(.data$startDay, .data$timeId, .data$endDay)
  
  temporalChoices <- dplyr::bind_rows(
    temporalChoices %>% dplyr::slice(0),
    dplyr::tibble(
      timeId = -1,
      temporalChoices = "Time invariant",
      primaryTimeId = 1,
      isTemporal = 0
    ),
    temporalChoices
  ) %>%
    dplyr::mutate(sequence = dplyr::row_number())
  
  return(temporalChoices)
}


#' Cohort Diagnostics Explorer main module
#' @param id                            module Id
#' @param connectionHandler             ResultModelManager ConnectionHander instance
#' @param resultDatabaseSettings        results database settings
#' @param dataSource                    dataSource optionally created with createCdDatabaseDataSource
#'
#' @family CohortDiagnostics
#' @export
cohortDiagnosticsServer <- function(id,
                                    connectionHandler,
                                    resultDatabaseSettings,
                                    dataSource = NULL) {
  ns <- shiny::NS(id)
  
  checkmate::assertClass(dataSource, "CdDataSource", null.ok = TRUE)
  if (is.null(dataSource)) {
    checkmate::assertR6(connectionHandler, "ConnectionHandler", null.ok = FALSE)
    dataSource <-
      createCdDatabaseDataSource(
        connectionHandler = connectionHandler,
        resultDatabaseSettings = resultDatabaseSettings,
        displayProgress = TRUE
      )
  }
  
  shiny::moduleServer(id, function(input, output, session) {
    databaseTable <- dataSource$dbTable
    cohortTable <- dataSource$cohortTable
    conceptSets <- dataSource$conceptSets
    cohortCountTable <- dataSource$cohortCountTable
    enabledReports <- dataSource$enabledReports
    temporalChoices <- dataSource$temporalChoices
    temporalCharacterizationTimeIdChoices <- dataSource$temporalCharacterizationTimeIdChoices
    
    shiny::observe({
      
      selection <- c(
        "Cohort Definitions" = "cohortDefinitions",
        "Database Information" = "databaseInformation"
      )
      if ("cohortCount" %in% dataSource$enabledReports)
        selection["Cohort Counts"] <- "cohortCounts"
      
      if ("indexEvents" %in% dataSource$enabledReports)
        selection["Index Events"] <- "indexEvents"
      
      if ("temporalCovariateValue" %in% dataSource$enabledReports) {
        selection["Cohort Characterization"] <- "characterization"
        selection["Compare Cohort Characterization"] <- "compareCohortCharacterization"
        selection["Time Distributions"] <- "timeDistribution"
        selection["Cohort Overlap"] <- "cohortOverlap"
      }

      if ("cohortInclusion" %in% dataSource$enabledReports)
        selection["Inclusion Rule Statistics"] <- "inclusionRules"
      
      if ("incidenceRate" %in% dataSource$enabledReports)
        selection["Incidence"] <- "incidenceRates"
      
      if ("visitContext" %in% dataSource$enabledReports)
        selection["Visit Context"] <- "visitContext"
      
      if ("includedSourceConcept" %in% dataSource$enabledReports)
        selection["Concepts In Data Source"] <- "conceptsInDataSource"
      
      if ("orphanConcept" %in% dataSource$enabledReports)
        selection["Orphan Concepts"] <- "orphanConcepts"
      
      if ("indexEventBreakdown" %in% dataSource$enabledReports)
        selection["Index Event Breakdown"] <- "indexEvents"
      
      shiny::updateSelectInput(
        inputId = "tabs",
        label = "Select Report",
        choices = selection,
        selected = c("cohortDefinitions")
      )
    })
    
    # Reacive: targetCohortId
    targetCohortId <- shiny::reactive({
      return(cohortTable$cohortId[cohortTable$compoundName == input$targetCohort])
    })
    # Reacive: cohortIds
    cohortIds <- shiny::reactive({
      cohortTable %>%
        dplyr::filter(.data$compoundName %in% input$cohorts) %>%
        dplyr::select("cohortId") %>%
        dplyr::pull()
    })
    
    selectedConceptSets <- shiny::reactive({
      input$conceptSetsSelected
    })
    
    # conceptSetIds ----
    conceptSetIds <- shiny::reactive(x = {
      conceptSetsFiltered <- conceptSets %>%
        dplyr::filter(.data$conceptSetName %in% selectedConceptSets()) %>%
        dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
        dplyr::select("conceptSetId") %>%
        dplyr::pull() %>%
        unique()
      return(conceptSetsFiltered)
    })
    
    databaseChoices <- databaseTable$databaseId
    names(databaseChoices) <- databaseTable$databaseName
    
    ## ReactiveValue: selectedDatabaseIds ----
    selectedDatabaseIds <- shiny::reactive({
      if (!is.null(input$tabs)) {
        if (input$tabs %in% c(
          "compareCohortCharacterization",
          "compareTemporalCharacterization",
          "temporalCharacterization",
          "databaseInformation"
        )) {
          return(input$database)
        } else {
          return(input$databases)
        }
      }
    })
    
    
    shiny::observe({
      shinyWidgets::updatePickerInput(session = session,
                                      inputId = "database",
                                      choices = databaseChoices,
                                      selected = databaseChoices[[1]],
      )
      shinyWidgets::updatePickerInput(session = session,
                                      inputId = "databases",
                                      choices = databaseChoices,
                                      selected = databaseChoices[[1]],
      )
    })
    
    ## ReactiveValue: selectedTemporalTimeIds ----
    selectedTemporalTimeIds <- shiny::reactiveVal(NULL)
    shiny::observeEvent(eventExpr = {
      list(
        input$timeIdChoices_open,
        input$timeIdChoices,
        input$tabs
      )
    }, handlerExpr = {
      if (isFALSE(input$timeIdChoices_open) ||
          !is.null(input$tabs) & !is.null(temporalCharacterizationTimeIdChoices)) {
        selectedTemporalTimeIds(
          temporalCharacterizationTimeIdChoices %>%
            dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
            dplyr::pull("timeId") %>%
            unique() %>%
            sort()
        )
      }
    })
    
    cohortSubset <- shiny::reactive({
      return(cohortTable %>%
               dplyr::arrange(.data$cohortId))
    })
    
    shiny::observe({
      subset <- cohortSubset()$compoundName
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "targetCohort",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset
      )
    })
    
    shiny::observe({
      subset <- cohortSubset()$compoundName
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "cohorts",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = c(subset[1], subset[2])
      )
    })
    
    # Characterization (Shared across) -------------------------------------------------
    ## Reactive objects ----
    ### getConceptSetNameForFilter ----
    getConceptSetNameForFilter <- shiny::reactive(x = {
      if (!hasData(targetCohortId())) {
        return(NULL)
      }
      if (sum(c('cohortId','conceptSetName') %in% colnames(dataSource$conceptSets)) !=2) {
        return(NULL)
      }
      dataSource$conceptSets %>%
        dplyr::filter(.data$cohortId == targetCohortId()) %>%
        dplyr::mutate(name = .data$conceptSetName) %>%
        dplyr::select("name")
      
    })
    
    shiny::observe({
      subset <- getConceptSetNameForFilter()$name %>%
        sort() %>%
        unique()
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "conceptSetsSelected",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset
      )
    })
    
    selectedCohorts <- shiny::reactive({
      cohorts <- cohortSubset() %>%
        dplyr::filter(.data$cohortId %in% cohortIds()) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::select("compoundName")
      return(apply(cohorts, 1, function(x) {
        shiny::tags$tr(lapply(x, shiny::tags$td))
      }))
    })
    
    selectedCohort <- shiny::reactive({
      return(input$targetCohort)
    })
    
    if ("cohort" %in% enabledReports) {
      cohortDefinitionsModule(id = "cohortDefinitions",
                              dataSource = dataSource,
                              cohortDefinitions = cohortSubset)
    }
    
    if ("includedSourceConcept" %in% enabledReports) {
      conceptsInDataSourceModule(id = "conceptsInDataSource",
                                 dataSource = dataSource,
                                 selectedCohort = selectedCohort,
                                 selectedDatabaseIds = selectedDatabaseIds,
                                 targetCohortId = targetCohortId,
                                 selectedConceptSets = selectedConceptSets,
                                 conceptSetIds = conceptSetIds,
                                 databaseTable = databaseTable)
    }
    
    if ("orphanConcept" %in% enabledReports) {
      orphanConceptsModule("orphanConcepts",
                           dataSource = dataSource,
                           databaseTable = databaseTable,
                           selectedCohort = selectedCohort,
                           selectedDatabaseIds = selectedDatabaseIds,
                           targetCohortId = targetCohortId,
                           selectedConceptSets = selectedConceptSets,
                           conceptSetIds = conceptSetIds)
    }
    
    if ("cohortCount" %in% enabledReports) {
      cohortCountsModule(id = "cohortCounts",
                         dataSource = dataSource,
                         cohortTable = cohortTable, # The injection of tables like this should be removed
                         databaseTable = databaseTable, # The injection of tables like this should be removed
                         selectedCohorts = selectedCohorts,
                         selectedDatabaseIds = selectedDatabaseIds,
                         cohortIds = cohortIds)
    }
    
    if ("indexEventBreakdown" %in% enabledReports) {
      indexEventBreakdownModule(id = "indexEvents",
                                dataSource = dataSource,
                                selectedCohort = selectedCohort,
                                targetCohortId = targetCohortId,
                                cohortCountTable = cohortCountTable,
                                selectedDatabaseIds = selectedDatabaseIds)
    }
    
    if ("visitContext" %in% enabledReports) {
      visitContextModule(id = "visitContext",
                         dataSource = dataSource,
                         selectedCohort = selectedCohort,
                         selectedDatabaseIds = selectedDatabaseIds,
                         targetCohortId = targetCohortId,
                         cohortCountTable = cohortCountTable,
                         databaseTable = databaseTable)
    }
    
    if ("temporalCovariateValue" %in% enabledReports) {
      cohortOverlapModule(id = "cohortOverlap",
                          dataSource = dataSource,
                          selectedCohorts = selectedCohorts,
                          selectedDatabaseIds = selectedDatabaseIds,
                          targetCohortId = targetCohortId,
                          cohortIds = cohortIds,
                          cohortTable = cohortTable)
    }
    
    if ("temporalCovariateValue" %in% enabledReports) {
      timeDistributionsModule(id = "timeDistributions",
                              dataSource = dataSource,
                              selectedCohorts = selectedCohorts,
                              cohortIds = cohortIds,
                              selectedDatabaseIds = selectedDatabaseIds)
      
      cohortDiagCharacterizationModule(id = "characterization",
                                       dataSource = dataSource)
      
      compareCohortCharacterizationModule(id = "compareCohortCharacterization",
                                          dataSource = dataSource)
    }
    
    if ("incidenceRate" %in% enabledReports) {
      incidenceRatesModule(id = "incidenceRates",
                           dataSource = dataSource,
                           selectedCohorts = selectedCohorts,
                           cohortIds = cohortIds,
                           selectedDatabaseIds = selectedDatabaseIds,
                           databaseTable = databaseTable,
                           cohortTable = cohortTable)
    }
    
    if ("cohortInclusion" %in% enabledReports) {
      inclusionRulesModule(id = "inclusionRules",
                           dataSource = dataSource,
                           databaseTable = databaseTable,
                           selectedCohort = selectedCohort,
                           targetCohortId = targetCohortId,
                           selectedDatabaseIds = selectedDatabaseIds)
      
    }
    databaseInformationModule(id = "databaseInformation",
                              dataSource = dataSource,
                              selectedDatabaseIds = selectedDatabaseIds,
                              databaseTable = databaseTable)
    
  }
  
  )
  
}
