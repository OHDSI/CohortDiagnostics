# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of PatientLevelPrediction
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


#' database Information View
#' @description
#' Use for customizing UI
#'
#' @param id    Namespace Id - use namespaced id ns("databaseInformation") inside diagnosticsExplorer module
#' @family CohortDiagnostics
#' @export
databaseInformationView <- function(id) {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      width = NULL,
      title = "Execution meta-data",
      shiny::tags$p("Each entry relates to execution on a given CDM. Results are merged between executions incrementally"),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("databaseInformationTable"))),
      shiny::conditionalPanel(
        "output.databaseInformationTableIsSelected == true",
        ns = ns,
        shinydashboard::box(
          title = shiny::htmlOutput(outputId = ns("metadataInfoTitle")),
          collapsible = TRUE,
          width = NULL,
          collapsed = FALSE,
          shiny::htmlOutput(outputId = ns("metadataInfoDetailsText")),
          shinydashboard::box(
            title = NULL,
            collapsible = TRUE,
            width = NULL,
            collapsed = FALSE,
            shiny::verbatimTextOutput(outputId = ns("argumentsAtDiagnosticsInitiationJson")),
            shiny::tags$head(
              shiny::tags$style("#argumentsAtDiagnosticsInitiationJson { max-height:400px};")
            )
          )
        )
      )
    )
  )
}


getMetaDataResults <- function(dataSource, databaseId) {
  sql <- "SELECT *
              FROM  @schema.@metadata
              WHERE database_id = @database_id;"

  data <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      metadata = dataSource$prefixTable("metadata"),
      schema = dataSource$schema,
      database_id = quoteLiterals(databaseId)
    ) %>%
      tidyr::tibble()

  return(data)
}


getExecutionMetadata <- function(dataSource, databaseId) {
  databaseMetadata <-
    getMetaDataResults(dataSource, databaseId)

  if (!hasData(databaseMetadata)) {
    return(NULL)
  }
  columnNames <-
    databaseMetadata$variableField %>%
      unique() %>%
      sort()
  columnNamesNoJson <-
    columnNames[stringr::str_detect(
      string = tolower(columnNames),
      pattern = "json",
      negate = TRUE
    )]
  columnNamesJson <-
    columnNames[stringr::str_detect(
      string = tolower(columnNames),
      pattern = "json",
      negate = FALSE
    )]

  transposeNonJsons <- databaseMetadata %>%
    dplyr::filter(.data$variableField %in% c(columnNamesNoJson)) %>%
    dplyr::rename(name = "variableField") %>%
    dplyr::group_by(.data$databaseId, .data$startTime, .data$name) %>%
    dplyr::summarise(
      valueField = max(.data$valueField),
      .groups = "keep"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      names_from = "name",
      values_from = "valueField"
    ) %>%
    dplyr::mutate(startTime = stringr::str_replace(
      string = .data$startTime,
      pattern = "TM_",
      replacement = ""
    ))

  transposeNonJsons$startTime <-
    transposeNonJsons$startTime %>% as.POSIXct()

  transposeJsons <- databaseMetadata %>%
    dplyr::filter(.data$variableField %in% c(columnNamesJson)) %>%
    dplyr::rename(name = "variableField") %>%
    dplyr::group_by(.data$databaseId, .data$startTime, .data$name) %>%
    dplyr::summarise(
      valueField = max(.data$valueField),
      .groups = "keep"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      names_from = "name",
      values_from = "valueField"
    ) %>%
    dplyr::mutate(startTime = stringr::str_replace(
      string = .data$startTime,
      pattern = "TM_",
      replacement = ""
    ))

  transposeJsons$startTime <-
    transposeJsons$startTime %>% as.POSIXct()

  transposeJsonsTemp <- list()
  for (i in (1:nrow(transposeJsons))) {
    transposeJsonsTemp[[i]] <- transposeJsons[i,]
    for (j in (1:length(columnNamesJson))) {
      transposeJsonsTemp[[i]][[columnNamesJson[[j]]]] <-
        transposeJsonsTemp[[i]][[columnNamesJson[[j]]]] %>%
          jsonlite::fromJSON() %>%
          jsonlite::toJSON(digits = 23, pretty = TRUE, auto_unbox = TRUE)
    }
  }
  transposeJsons <- dplyr::bind_rows(transposeJsonsTemp)
  data <- transposeNonJsons %>%
    dplyr::left_join(transposeJsons,
                     by = c("databaseId", "startTime")
    )
  if ("observationPeriodMaxDate" %in% colnames(data)) {
    data$observationPeriodMaxDate <-
      tryCatch(
        expr = as.Date(data$observationPeriodMaxDate),
        error = data$observationPeriodMaxDate
      )
  }
  if ("observationPeriodMinDate" %in% colnames(data)) {
    data$observationPeriodMinDate <-
      tryCatch(
        expr = as.Date(data$observationPeriodMinDate),
        error = data$observationPeriodMinDate
      )
  }
  if ("sourceReleaseDate" %in% colnames(data)) {
    data$sourceReleaseDate <-
      tryCatch(
        expr = as.Date(data$sourceReleaseDate),
        error = data$sourceReleaseDate
      )
  }
  if ("personDaysInDatasource" %in% colnames(data)) {
    data$personDaysInDatasource <-
      tryCatch(
        expr = as.numeric(data$personDaysInDatasource),
        error = data$personDaysInDatasource
      )
  }
  if ("recordsInDatasource" %in% colnames(data)) {
    data$recordsInDatasource <-
      tryCatch(
        expr = as.numeric(data$recordsInDatasource),
        error = data$recordsInDatasource
      )
  }
  if ("personDaysInDatasource" %in% colnames(data)) {
    data$personDaysInDatasource <-
      tryCatch(
        expr = as.numeric(data$personDaysInDatasource),
        error = data$personDaysInDatasource
      )
  }
  if ("runTime" %in% colnames(data)) {
    data$runTime <-
      tryCatch(
        expr = round(as.numeric(data$runTime), digits = 1),
        error = data$runTime
      )
  }
  return(data)
}


getDatabaseMetadata <- function(dataSource, databaseTable) {
  data <- loadResultsTable(dataSource, "metadata", required = TRUE, cdTablePrefix = dataSource$cdTablePrefix, databaseTablePrefix = dataSource$databaseTablePrefix)
  data <- data %>%
    tidyr::pivot_wider(
      id_cols = c("startTime", "databaseId"),
      names_from = "variableField",
      values_from = "valueField"
    ) %>%
    dplyr::mutate(
      startTime = stringr::str_replace(
        string = .data$startTime,
        pattern = stringr::fixed("TM_"),
        replacement = ""
      )
    ) %>%
    dplyr::mutate(startTime = paste0(.data$startTime, " ", .data$timeZone)) %>%
    dplyr::mutate(startTime = as.POSIXct(.data$startTime)) %>%
    dplyr::group_by(
      .data$databaseId,
      .data$startTime
    ) %>%
    dplyr::arrange(.data$databaseId, dplyr::desc(.data$startTime), .by_group = TRUE) %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    dplyr::filter(.data$rn == 1) %>%
    dplyr::select(-"timeZone")

  if ("runTime" %in% colnames(data)) {
    data$runTime <- round(x = as.numeric(data$runTime), digits = 2)
  }
  if ("observationPeriodMinDate" %in% colnames(data)) {
    data$observationPeriodMinDate <-
      as.Date(data$observationPeriodMinDate)
  }
  if ("observationPeriodMaxDate" %in% colnames(data)) {
    data$observationPeriodMaxDate <-
      as.Date(data$observationPeriodMaxDate)
  }
  if ("personsInDatasource" %in% colnames(data)) {
    data$personsInDatasource <- as.numeric(data$personsInDatasource)
  }
  if ("recordsInDatasource" %in% colnames(data)) {
    data$recordsInDatasource <- as.numeric(data$recordsInDatasource)
  }
  if ("personDaysInDatasource" %in% colnames(data)) {
    data$personDaysInDatasource <-
      as.numeric(data$personDaysInDatasource)
  }
  colnamesOfInterest <-
    c(
      "startTime",
      "databaseId",
      "runTime",
      "runTimeUnits",
      "sourceReleaseDate",
      "cdmVersion",
      "cdmReleaseDate",
      "observationPeriodMinDate",
      "observationPeriodMaxDate",
      "personsInDatasource",
      "recordsInDatasource",
      "personDaysInDatasource"
    )

  commonColNames <- intersect(colnames(data), colnamesOfInterest)

  data <- data %>%
    dplyr::select(dplyr::all_of(commonColNames))

  databaseTable %>%
    dplyr::distinct() %>%
    dplyr::mutate(id = dplyr::row_number()) %>%
    dplyr::mutate(shortName = paste0("D", .data$id)) %>%
    dplyr::left_join(data,
                     by = "databaseId"
    ) %>%
    dplyr::relocate("id", "databaseId", "shortName")
}

# What this module does is incredibly simple. How it does it is not.
databaseInformationModule <- function(
    id,
    dataSource,
    selectedDatabaseIds,
    databaseTable = dataSource$dbTable
) {
  ns <- shiny::NS(id)

  ## Replace this pre-loading nonsense
  databaseMetadata <- getDatabaseMetadata(dataSource, databaseTable)

  shiny::moduleServer(id, function(input, output, session) {

    getDatabaseInformation <- shiny::reactive(x = {
      return(databaseMetadata %>% dplyr::filter(.data$databaseId %in% selectedDatabaseIds()))
    })

    # Output: databaseInformationTable ------------------------
    output$databaseInformationTable <- reactable::renderReactable(expr = {
      data <- getDatabaseInformation()
      shiny::validate(shiny::need(
        all(!is.null(data), nrow(data) > 0),
        "No data available for selected combination."
      ))

      if (!"vocabularyVersionCdm" %in% colnames(data)) {
        data$vocabularyVersionCdm <- "Not in data"
      }
      if (!"vocabularyVersion" %in% colnames(data)) {
        data$vocabularyVersion <- "Not in data"
      }

      keyColumns <- intersect(
        colnames(data),
        c(
          "databaseId",
          "databaseName",
          "vocabularyVersionCdm",
          "vocabularyVersion",
          "description",
          "startTime",
          "runTime",
          "runTimeUnits",
          "sourceReleaseDate",
          "cdmVersion",
          "cdmReleaseDate",
          "observationPeriodMinDate",
          "observationPeriodMaxDate"
        )
      )

      dataColumns <- c(
        "personsInDatasource",
        "recordsInDatasource",
        "personDaysInDatasource"
      )

      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        selection = "single"
      )
    })

    selectedDbRow <- shiny::reactive({
      reactable::getReactableState("databaseInformationTable", "selected")
    })

    output$databaseInformationTableIsSelected <- shiny::reactive({
      return(!is.null(selectedDbRow()))
    })

    shiny::outputOptions(output,
                  "databaseInformationTableIsSelected",
                  suspendWhenHidden = FALSE)

    getFilteredMetadataInformation <- shiny::reactive(x = {
      idx <- selectedDbRow()
      dbInfo <- getDatabaseInformation()[idx,]
      if (is.null(dbInfo)) {
        return(NULL)
      }
      data <- getExecutionMetadata(dataSource = dataSource,
                                   databaseId = dbInfo$databaseId)

       if (is.null(data)) {
        return(NULL)
      }

      # The meta-data data structure needs to be taken out!
      data <- data %>%
        dplyr::mutate(startTime = paste0(.data$startTime)) %>%
        dplyr::mutate(startTime = as.POSIXct(.data$startTime))

      data <- data %>% dplyr::filter(.data$startTime == dbInfo$startTime)
      return(data)
    })

    output$metadataInfoTitle <- shiny::renderUI(expr = {
      data <- getFilteredMetadataInformation()

      if (!hasData(data)) {
        return(NULL)
      }
      shiny::tags$p(paste(
        "Run on ",
        data$databaseId,
        "on ",
        data$startTime,
        " for ",
        data$runTime,
        " ",
        data$runTimeUnits
      ))
    })

    output$metadataInfoDetailsText <- shiny::renderUI(expr = {
      data <- getFilteredMetadataInformation()
      if (!hasData(data)) {
        return(NULL)
      }
      shiny::tags$table(shiny::tags$tr(shiny::tags$td(
        paste(
          "Ran for ",
          data$runTime,
          data$runTimeUnits,
          "on ",
          data$currentPackage,
          "(",
          data$currentPackageVersion,
          ")"
        )
      )))
    })


    ## output: argumentsAtDiagnosticsInitiationJson----
    output$argumentsAtDiagnosticsInitiationJson <-
      shiny::renderText(expr = {
        data <- getFilteredMetadataInformation()
        if (!hasData(data)) {
          return(NULL)
        }
        data <- data %>%
          dplyr::pull("argumentsAtDiagnosticsInitiationJson") %>%
          jsonlite::fromJSON() %>%
          jsonlite::toJSON(
            digits = 23,
            pretty = TRUE,
            auto_unbox = TRUE
          )
        return(data)
      })
  })
}
