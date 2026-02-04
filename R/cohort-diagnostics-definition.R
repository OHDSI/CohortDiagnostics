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

#' Returns list with circe generated documentation
#'
#' @description
#' Returns list with circe generated documentation
#'
#' @param cohortDefinition An R object (list) with a list representation of the cohort definition expression,
#'                          that may be converted to a cohort expression JSON using
#'                          jsonlite::toJSON(x = cohortDefinition, digits = 23, pretty = TRUE)
#'
#' @param cohortName Name for the cohort definition
#'
#' @param includeConceptSets Do you want to inclued concept set in the documentation
#' @family CohortDiagnostics
#' @return list object
#'
getCirceRenderedExpression <- function(cohortDefinition,
                                       cohortName = "Cohort Definition",
                                       includeConceptSets = FALSE) {
  circeExpression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortDefinition)
  circeExpressionMarkdown <-
    CirceR::cohortPrintFriendly(circeExpression)
  circeConceptSetListmarkdown <-
    CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)

  circeExpressionMarkdown <-
    paste0(
      "## Human Readable Cohort Definition",
      "\r\n\r\n",
      circeExpressionMarkdown
    )

  circeExpressionMarkdown <-
    paste0(
      "# ",
      cohortName,
      "\r\n\r\n",
      circeExpressionMarkdown
    )

  if (includeConceptSets) {
    circeExpressionMarkdown <-
      paste0(
        circeExpressionMarkdown,
        "\r\n\r\n",
        "\r\n\r\n",
        "## Concept Sets:",
        "\r\n\r\n",
        circeConceptSetListmarkdown
      )
  }

  htmlExpressionCohort <-
    markdown::renderMarkdown(text = circeExpressionMarkdown)
  htmlExpressionConceptSetExpression <-
    markdown::renderMarkdown(text = circeConceptSetListmarkdown)
  return(
    list(
      cohortJson = cohortDefinition,
      cohortMarkdown = circeExpressionMarkdown,
      conceptSetMarkdown = circeConceptSetListmarkdown,
      cohortHtmlExpression = htmlExpressionCohort,
      conceptSetHtmlExpression = htmlExpressionConceptSetExpression
    )
  )
}


copyToClipboardButton <-
  function(toCopyId,
           label = "Copy to clipboard",
           icon = shiny::icon("clipboard"),
           ...) {
    script <- sprintf(
      "
  text = document.getElementById('%s').textContent;
  html = document.getElementById('%s').innerHTML;
  function listener(e) {
    e.clipboardData.setData('text/html', html);
    e.clipboardData.setData('text/plain', text);
    e.preventDefault();
  }
  document.addEventListener('copy', listener);
  document.execCommand('copy');
  document.removeEventListener('copy', listener);
  return false;",
      toCopyId,
      toCopyId
    )

    shiny::tags$button(
      type = "button",
      class = "btn btn-default action-button",
      onclick = script,
      icon,
      label,
      ...
    )
  }


getConceptSetDetailsFromCohortDefinition <-
  function(cohortDefinitionExpression) {
    cohortDefinitionExpression <- jsonlite::fromJSON(cohortDefinitionExpression, simplifyDataFrame = FALSE)
    if ("expression" %in% names(cohortDefinitionExpression)) {
      expression <- cohortDefinitionExpression$expression
    } else {
      expression <- cohortDefinitionExpression
    }

    if (is.null(expression$ConceptSets)) {
      return(NULL)
    }

    # you don't want to have to go there
    conceptSetExpression <- list()
    mappedConceptSets <- list()
    for (i in seq_len(length(expression$ConceptSets))) {
      conceptSetExpression[[i]] <- list(
        id = expression$ConceptSets[[i]]$id |> as.character(),
        name = expression$ConceptSets[[i]]$name,
        json = jsonlite::toJSON(expression$ConceptSets[[i]]$expression) |> as.character()
      )

      items <- expression$ConceptSets[[i]]$expression$items
      conceptSet <- data.frame()
      # create concept set data frane
      for (j in seq_len(length(items))) {
        row <- items[[j]]$concept |> data.frame()
        colnames(row) <- colnames(row) |> SqlRender::snakeCaseToCamelCase()
        row$isExcluded = items[[j]]$isExcluded
        row$includeDescendants = items[[j]]$includeDescendants
        row$includeMapped = items[[j]]$includeMapped
        conceptSet <- dplyr::bind_rows(conceptSet, row)
      }
      mappedConceptSets[[as.character(conceptSetExpression[[i]]$id)]] <- conceptSet
    }


    output <- list(
      conceptSetExpression = dplyr::bind_rows(conceptSetExpression),
      conceptSetExpressionDetails = mappedConceptSets
    )
    return(output)
  }


getCdCohortRows <- function(dataSource, cohortIds) {
  sql <- "SELECT * FROM @schema.@cohort_table
    WHERE cohort_id IN (@cohort_ids)"
  dataSource$connectionHandler$queryDb(
    sql = sql,
    schema = dataSource$schema,
    cohort_table = dataSource$cohortTableName,
    cohort_ids = cohortIds
  )
}

exportCohortDefinitionsZip <- function(cohortDefinitions,
                                       zipFile = NULL) {
  rootFolder <-
    stringr::str_replace_all(
      string = Sys.time(),
      pattern = "-",
      replacement = ""
    )
  rootFolder <-
    stringr::str_replace_all(
      string = rootFolder,
      pattern = ":",
      replacement = ""
    )
  tempdir <- file.path(tempdir(), rootFolder)

  for (i in (1:nrow(cohortDefinitions))) {
    cohortId <- cohortDefinitions[i,]$cohortId
    dir.create(
      path = file.path(tempdir, cohortId),
      recursive = TRUE,
      showWarnings = FALSE
    )

    if (is.na(cohortDefinitions[i,]$subsetDefinitionId)) {
      cohortExpression <- cohortDefinitions[i,]$json

      details <-
        getCirceRenderedExpression(cohortDefinition = cohortExpression)

      SqlRender::writeSql(
        sql = details$cohortJson,
        targetFile = file.path(
          tempdir,
          cohortId,
          paste0("cohortDefinitionJson_", cohortId, ".json")
        )
      )
      SqlRender::writeSql(
        sql = details$cohortMarkdown,
        targetFile = file.path(
          tempdir,
          cohortId,
          paste0("cohortDefinitionMarkdown_", cohortId, ".md")
        )
      )

      SqlRender::writeSql(
        sql = details$conceptSetMarkdown,
        targetFile = file.path(
          tempdir,
          cohortId,
          paste0("conceptSetMarkdown_", cohortId, ".md")
        )
      )

      SqlRender::writeSql(
        sql = details$cohortHtmlExpression,
        targetFile = file.path(
          tempdir,
          cohortId,
          paste0("cohortDefinitionHtml_", cohortId, ".html")
        )
      )

      SqlRender::writeSql(
        sql = details$conceptSetHtmlExpression,
        targetFile = file.path(
          tempdir,
          cohortId,
          paste0("conceptSetsHtml_", cohortId, ".html")
        )
      )
    }
  }

  return(DatabaseConnector::createZipFile(zipFile = zipFile,
                                          files = tempdir,
                                          rootFolder = tempdir))
}

#' Cohort Definitions View
#' @description
#' Outputs cohort definitions
#' @param id            Namespace id for module
#' @family CohortDiagnostics
#' @export
cohortDefinitionsView <- function(id) {
  ns <- shiny::NS(id)
  ui <- shiny::tagList(
    shinydashboard::box(
      width = NULL,
      status = "primary",
      shiny::withTags(
        table(width = "100%",
              shiny::tags$tr(
                shiny::tags$td(align = "left",
                               shiny::h4("Cohort Definition")
                ),
                shiny::tags$td(
                  align = "right",
                  shiny::downloadButton(
                    outputId = ns("exportAllCohortDetails"),
                    label = "Export Cohorts Zip",
                    icon = shiny::icon("file-export"),
                    style = "margin-top: 5px; margin-bottom: 5px;"
                  )
                )
              )
        )
      ),
      shiny::column(12,
                   shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("cohortDefinitionTable")))),
      shiny::column(
        12,
        shiny::conditionalPanel(
          "output.cohortDefinitionRowIsSelected == true",
          ns = ns,
          shiny::tabsetPanel(
            type = "tab",
            shiny::tabPanel(title = "Details",
                            shiny::htmlOutput(ns("cohortDetailsText"))),
            shiny::tabPanel(title = "Cohort Count",
                            shiny::tags$br(),
                            reactable::reactableOutput(outputId = ns("cohortCountsTableInCohortDefinition"))),
            shiny::tabPanel(title = "Cohort definition",
                            copyToClipboardButton(toCopyId = ns("cohortDefinitionText"),
                                                  style = "margin-top: 5px; margin-bottom: 5px;"),
                            shiny::htmlOutput(ns("cohortDefinitionText"))),
            shiny::tabPanel(
              title = "Concept Sets",
              reactable::reactableOutput(outputId = ns("conceptsetExpressionsInCohort")),
              shiny::conditionalPanel(
                condition = "output.cohortDefinitionConceptSetExpressionRowIsSelected == true",
                ns = ns,
                shiny::tags$table(
                  shiny::tags$tr(
                    shiny::tags$td(
                      shiny::radioButtons(
                        inputId = ns("conceptSetsType"),
                        label = "",
                        choices = c("Concept Set Expression",
                                    "Resolved",
                                    "Mapped",
                                    "Json"),
                        selected = "Concept Set Expression",
                        inline = TRUE
                      )
                    ),
                    shiny::tags$td(
                      shiny::conditionalPanel(
                        condition = "input.conceptSetsType == 'Resolved' | input.conceptSetsType == 'Mapped'",
                        ns = ns,
                        shiny::selectInput(ns("vocabularySelection"),
                                           label = "Database:",
                                           width = 400,
                                           choices = c())
                      )
                    ),
                    shiny::tags$td(
                      shiny::htmlOutput(ns("subjectCountInCohortConceptSet"))
                    ),
                    shiny::tags$td(
                      shiny::htmlOutput(ns("recordCountInCohortConceptSet"))
                    )
                  )
                )
              ),
              shiny::conditionalPanel(
                ns = ns,
                condition = "output.cohortDefinitionConceptSetExpressionRowIsSelected == true &
                  input.conceptSetsType != 'Resolved' &
                  input.conceptSetsType != 'Mapped' &
                  input.conceptSetsType != 'Json'",
                shiny::tags$p("Filter logical values with \"T\" and \"F\""),
                shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("cohortDefinitionConceptSetDetailsTable")))
              ),
              shiny::conditionalPanel(
                ns = ns,
                condition = "input.conceptSetsType == 'Resolved'",
                shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("cohortDefinitionResolvedConceptsTable")))
              ),
              shiny::conditionalPanel(
                ns = ns,
                condition = "input.conceptSetsType == 'Mapped'",
                shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("cohortDefinitionMappedConceptsTable")))
              ),
              shiny::conditionalPanel(
                condition = "input.conceptSetsType == 'Json'",
                copyToClipboardButton(toCopyId = ns("cohortConceptsetExpressionJson"),
                                      style = "margin-top: 5px; margin-bottom: 5px;"),
                shiny::verbatimTextOutput(outputId = ns("cohortConceptsetExpressionJson")),
                shiny::tags$head(
                  shiny::tags$style("#cohortConceptsetExpressionJson { max-height:400px};")
                ),
                ns = ns
              )
            ),
            shiny::tabPanel(
              title = "JSON",
              copyToClipboardButton(ns("cohortDefinitionJson"), style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput(ns("cohortDefinitionJson")),
              shiny::tags$head(
                shiny::tags$style("#cohortDefinitionJson { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "SQL",
              copyToClipboardButton(ns("cohortDefinitionSql"), style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput(ns("cohortDefinitionSql")),
              shiny::tags$head(
                shiny::tags$style("#cohortDefinitionSql { max-height:400px};")
              )
            )
          )
        )
      )
    )
  )
  ui
}


getCountForConceptIdInCohort <-
  function(dataSource,
           cohortId,
           databaseIds) {
    sql <- "SELECT ics.*
            FROM  @schema.@table_name ics
            WHERE ics.cohort_id = @cohort_id
             AND database_id in (@database_ids);"
    data <-
      dataSource$connectionHandler$queryDb(
        sql = sql,
        schema = dataSource$schema,
        cohort_id = cohortId,
        database_ids = quoteLiterals(databaseIds),
        table_name = dataSource$prefixTable("included_source_concept"),
        snakeCaseToCamelCase = TRUE
      ) %>%
        tidyr::tibble()

    standardConceptId <- data %>%
      dplyr::select(
        "databaseId",
        "conceptId",
        "conceptSubjects",
        "conceptCount"
      ) %>%
      dplyr::group_by(
        .data$databaseId,
        .data$conceptId
      ) %>%
      dplyr::summarise(
        conceptSubjects = max(.data$conceptSubjects),
        conceptCount = sum(.data$conceptCount),
        .groups = "keep"
      ) %>%
      dplyr::ungroup()


    sourceConceptId <- data %>%
      dplyr::select(
        "databaseId",
        "sourceConceptId",
        "conceptSubjects",
        "conceptCount"
      ) %>%
      dplyr::rename("conceptId" = "sourceConceptId") %>%
      dplyr::group_by(
        .data$databaseId,
        .data$conceptId
      ) %>%
      dplyr::summarise(
        conceptSubjects = max(.data$conceptSubjects),
        conceptCount = sum(.data$conceptCount),
        .groups = "keep"
      ) %>%
      dplyr::ungroup()

    data <- dplyr::bind_rows(
      standardConceptId,
      sourceConceptId %>%
        dplyr::anti_join(
          y = standardConceptId %>%
            dplyr::select("databaseId", "conceptId"),
          by = c("databaseId", "conceptId")
        )
    ) %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$databaseId, .data$conceptId)

    return(data)
  }


#' Cohort Definition module
#' @description
#' cohort defintion conceptsets, json etc
#'
#' @param id                            Namespace id
#' @param dataSource                    DatabaseConnection
#' @param cohortDefinitions             reactive of cohort definitions to display
#' @param databaseTable                 data.frame of databasese, databaseId, name
#' @param cohortTable                   data.frame of cohorts, cohortId, cohortName
#' @param cohortCountTable              data.frame of cohortCounts, cohortId, subjects records
#' @family CohortDiagnostics
cohortDefinitionsModule <- function(
  id,
  dataSource,
  cohortDefinitions,
  cohortTable = dataSource$cohortTable,
  cohortCountTable = dataSource$cohortCountTable,
  databaseTable = dataSource$dbTable
) {
  ns <- shiny::NS(id)

  cohortDefinitionServer <- function(input, output, session) {

    cohortDefinitionTableData <- shiny::reactive(x = {
      data <- cohortDefinitions() %>%
        dplyr::select("cohortId", "cohortName")
      return(data)
    })

    # Cohort Definition ---------------------------------------------------------
    output$cohortDefinitionTable <-
      reactable::renderReactable(expr = {

        shiny::withProgress({
          data <- cohortDefinitionTableData() %>%
            dplyr::mutate(cohortId = as.character(.data$cohortId))

          shiny::validate(shiny::need(hasData(data), "There is no data for this cohort."))
          keyColumns <- c("cohortId", "cohortName")
          dataColumns <- c()

          displayTable <- getDisplayTableSimple(
            data = data,
            databaseTable = databaseTable,
            keyColumns = keyColumns,
            dataColumns = dataColumns,
            selection = "single"
          )
        }, message = "Loading cohort definitions")
        return(displayTable)
      })

    selectedCohortDefinitionRow <- shiny::reactive({
      idx <- reactable::getReactableState("cohortDefinitionTable", "selected")
      if (is.null(idx)) {
        return(NULL)
      } else {
        subset <- cohortDefinitions()
        if (nrow(subset) == 0) {
          return(NULL)
        }
        row <- subset[idx[1],]
        return(getCdCohortRows(dataSource, row$cohortId))
      }
    })

    output$cohortDefinitionRowIsSelected <- shiny::reactive({
      return(!is.null(selectedCohortDefinitionRow()))
    })

    shiny::outputOptions(output,
                         "cohortDefinitionRowIsSelected",
                         suspendWhenHidden = FALSE)

    ## cohortDetailsText ---------------------------------------------------------
    output$cohortDetailsText <- shiny::renderUI({
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      } else {
        shiny::tags$table(
          style = "margin-top: 5px;",
          shiny::tags$tr(
            shiny::tags$td(shiny::tags$strong("Cohort ID: ")),
            shiny::tags$td(shiny::HTML("&nbsp;&nbsp;")),
            shiny::tags$td(row$cohortId)
          ),
          shiny::tags$tr(
            shiny::tags$td(shiny::tags$strong("Cohort Name: ")),
            shiny::tags$td(shiny::HTML("&nbsp;&nbsp;")),
            shiny::tags$td(row$cohortName)
          )
        )
      }
    })


    ## cohortCountsTableInCohortDefinition ---------------------------------------------------------
    output$cohortCountsTableInCohortDefinition <-
      reactable::renderReactable(expr = {
        if (is.null(selectedCohortDefinitionRow())) {
          return(NULL)
        }
        data <- cohortCountTable
        if (!hasData(data)) {
          return(NULL)
        }

        data <- data %>%
          dplyr::inner_join(databaseTable, by = "databaseId") %>%
          dplyr::filter(.data$cohortId == selectedCohortDefinitionRow()$cohortId) %>%
          dplyr::filter(.data$databaseId %in% databaseTable$databaseId) %>%
          dplyr::select("databaseName",
                        "cohortSubjects",
                        "cohortEntries") %>%
          dplyr::rename("persons" = "cohortSubjects",
                        "events" = "cohortEntries")

        shiny::validate(shiny::need(hasData(data), "There is no data for this cohort."))

        keyColumns <- c("databaseName")
        dataColumns <- c("persons", "events")

        displayTable <- getDisplayTableSimple(data = data,
                                              keyColumns = keyColumns,
                                              dataColumns = dataColumns)
        return(displayTable)
      })

    ## cohortDefinitionCirceRDetails ---------------------------------------------------------
    cohortDefinitionCirceRDetails <- shiny::reactive(x = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = "Rendering human readable cohort description using CirceR (may take time)", value = 0)
      data <- selectedCohortDefinitionRow()
      if (!hasData(data)) {
        return(NULL)
      }
      if (is.na(data$subsetDefinitionId[1])) {
        details <-
          getCirceRenderedExpression(
            cohortDefinition = data$json[1],
            cohortName = data$cohortName[1],
            includeConceptSets = TRUE
          )
      } else {
        details <- list(cohortHtmlExpression = paste("<p> Subset of cohort", data$subsetParent[1], "</p>"))
      }
      return(details)
    })

    output$cohortDefinitionText <- shiny::renderUI(expr = {
      cohortDefinitionCirceRDetails()$cohortHtmlExpression %>% shiny::HTML()
    })
    ## cohortDefinitionJson ---------------------------------------------------------
    output$cohortDefinitionJson <- shiny::renderText({
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      } else {
        row$json
      }
    })

    ## cohortDefinitionSql ---------------------------------------------------------
    output$cohortDefinitionSql <- shiny::renderText({
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      } else {
        row$sql
      }
    })

    ## cohortDefinitionConceptSetExpression ---------------------------------------------------------
    cohortDefinitionConceptSetExpression <- shiny::reactive({
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      }

      if (!is.na(row$subsetDefinitionId[1])) {
        return(NULL)
      }

      expression <- jsonlite::fromJSON(row$json)
      if (is.null(expression)) {
        return(NULL)
      }
      expression <-
        getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = row$json)

      return(expression)
    })

    output$conceptsetExpressionsInCohort <- reactable::renderReactable(expr = {
      data <- cohortDefinitionConceptSetExpression()
      if (is.null(data)) {
        return(NULL)
      }
      if (!is.null(data$conceptSetExpression) &&
        nrow(data$conceptSetExpression) > 0) {
        data <- data$conceptSetExpression %>%
          dplyr::select("id", "name")
      } else {
        return(NULL)
      }

      shiny::validate(shiny::need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no data for this cohort."
      ))

      data <- data %>% dplyr::mutate() # why do this?

      keyColumns <- c("id", "name")
      dataColumns <- c()
      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        selection = "single"
      )
    })

    ### cohortDefinitionConceptSetExpressionSelected ---------------------------------------------------------
    cohortDefinitionConceptSetExpressionSelected <- shiny::reactive(x = {
      idx <- reactable::getReactableState("conceptsetExpressionsInCohort", "selected")
      if (length(idx) == 0 || is.null(idx)) {
        return(NULL)
      }
      if (hasData(cohortDefinitionConceptSetExpression()$conceptSetExpression)) {
        data <-
          cohortDefinitionConceptSetExpression()$conceptSetExpression[idx,]
        if (!is.null(data)) {
          return(data)
        } else {
          return(NULL)
        }
      }
    })

    output$cohortDefinitionConceptSetExpressionRowIsSelected <- shiny::reactive(x = {
      return(!is.null(cohortDefinitionConceptSetExpressionSelected()))
    })

    shiny::outputOptions(x = output,
                         name = "cohortDefinitionConceptSetExpressionRowIsSelected",
                         suspendWhenHidden = FALSE)

    output$isDataSourceEnvironment <- shiny::reactive(x = {
      return(inherits(dataSource, "environment"))
    })
    shiny::outputOptions(x = output,
                         name = "isDataSourceEnvironment",
                         suspendWhenHidden = FALSE)

    ### cohortDefinitionConceptSetDetails ---------------------------------------------------------
    cohortDefinitionConceptSetDetails <- shiny::reactive(x = {
      if (is.null(cohortDefinitionConceptSetExpressionSelected())) {
        return(NULL)
      }
      data <-
        cohortDefinitionConceptSetExpression()$conceptSetExpressionDetails
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data[[cohortDefinitionConceptSetExpressionSelected()$id]]
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::select(
          "conceptId",
          "conceptName",
          "isExcluded",
          "includeDescendants",
          "includeMapped",
          "standardConcept",
          "invalidReason",
          "conceptCode",
          "domainId",
          "vocabularyId",
          "conceptClassId"
        )
      return(data)
    })

    output$cohortDefinitionConceptSetDetailsTable <-
      reactable::renderReactable(expr = {
        data <- cohortDefinitionConceptSetDetails()
        shiny::validate(shiny::need(
          all(!is.null(data),
              nrow(data) > 0),
          "There is no data for this cohort."
        ))
        if (is.null(cohortDefinitionConceptSetDetails())) {
          return(NULL)
        }

        data <- data %>%
          dplyr::rename("exclude" = "isExcluded",
                        "descendants" = "includeDescendants",
                        "mapped" = "includeMapped",
                        "invalid" = "invalidReason")
        shiny::validate(shiny::need(
          all(!is.null(data),
              nrow(data) > 0),
          "There is no data for this cohort."
        ))

        keyColumns <- c(
          "conceptId",
          "conceptName",
          "exclude",
          "descendants",
          "mapped",
          "standardConcept",
          "invalid",
          "conceptCode",
          "domainId",
          "vocabularyId",
          "conceptClassId"
        )

        dataColumns <- c()
        getDisplayTableSimple(data = data,
                              keyColumns = keyColumns,
                              dataColumns = dataColumns)

      })

    getDatabaseIdInCohortConceptSet <- shiny::reactive({
      return(databaseTable$databaseId[databaseTable$databaseIdWithVocabularyVersion == input$vocabularySchema])
    })

    ## Cohort Concept Set
    ### getSubjectAndRecordCountForCohortConceptSet ---------------------------------------------------------
    getSubjectAndRecordCountForCohortConceptSet <- shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()

      if (is.null(row) || length(getDatabaseIdInCohortConceptSet()) == 0) {
        return(NULL)
      } else {
        data <- cohortCountTable %>%
          dplyr::filter(.data$cohortId == row$cohortId) %>%
          dplyr::filter(.data$databaseId == getDatabaseIdInCohortConceptSet()) %>%
          dplyr::select("cohortSubjects", "cohortEntries")

        if (nrow(data) == 0) {
          return(NULL)
        } else {
          return(data)
        }
      }
    })

    ### subjectCountInCohortConceptSet ---------------------------------------------------------
    output$subjectCountInCohortConceptSet <- shiny::renderUI({
      row <- getSubjectAndRecordCountForCohortConceptSet()
      if (is.null(row)) {
        return(NULL)
      } else {
        shiny::tags$table(
          shiny::tags$tr(
            shiny::tags$td("Persons: "),
            shiny::tags$td(scales::comma(row$cohortSubjects, accuracy = 1))
          )
        )
      }
    })

    ### recordCountInCohortConceptSet ---------------------------------------------------------
    output$recordCountInCohortConceptSet <- shiny::renderUI({
      row <- getSubjectAndRecordCountForCohortConceptSet()
      if (is.null(row)) {
        return(NULL)
      } else {
        shiny::tags$table(
          shiny::tags$tr(
            shiny::tags$td("Records: "),
            shiny::tags$td(scales::comma(row$cohortEntries, accuracy = 1))
          )
        )
      }
    })

    ### getCohortDefinitionResolvedConceptsReactive ---------------------------------------------------------
    getCohortDefinitionResolvedConceptsReactive <-
      shiny::reactive(x = {
        row <- selectedCohortDefinitionRow()
        if (is.null(row)) {
          return(NULL)
        }
        output <-
          resolvedConceptSet(
            dataSource = dataSource,
            databaseIds = databaseTable$databaseId,
            cohortId = row$cohortId
          )
        if (!hasData(output)) {
          return(NULL)
        }
        conceptCount <- getCountForConceptIdInCohortReactive()
        output <- output %>%
          dplyr::left_join(conceptCount,
                           by = c("databaseId", "conceptId"))
        return(output)
      })

    output$cohortDefinitionResolvedConceptsTable <-
      reactable::renderReactable(expr = {
        if (input$conceptSetsType != 'Resolved') {
          return(NULL)
        }
        databaseIdToFilter <- databaseTable %>%
          dplyr::filter(.data$databaseIdWithVocabularyVersion == vocabSchema()) %>%
          dplyr::pull("databaseId")
        if (!hasData(databaseIdToFilter)) {
          return(NULL)
        }

        shiny::validate(shiny::need(
          length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
          "Please select concept set"
        ))

        data <- getCohortDefinitionResolvedConceptsReactive()
        shiny::validate(shiny::need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))
        data <- data %>%
          dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
          dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
          dplyr::rename("persons" = "conceptSubjects",
                        "records" = "conceptCount")
        shiny::validate(shiny::need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))
        keyColumns <- c(
          "conceptId",
          "conceptName",
          "domainId",
          "vocabularyId",
          "conceptClassId",
          "standardConcept",
          "conceptCode"
        )
        dataColumns <- c("persons",
                         "records")
        displayTable <- getDisplayTableSimple(data = data,
                                              keyColumns = keyColumns,
                                              dataColumns = dataColumns)
        return(displayTable)
      })


    ### getCountForConceptIdInCohortReactive ---------------------------------------------------------
    getCountForConceptIdInCohortReactive <-
      shiny::reactive(x = {
        row <- selectedCohortDefinitionRow()
        if (is.null(row)) {
          return(NULL)
        }
        data <- getCountForConceptIdInCohort(
          dataSource = dataSource,
          databaseIds = databaseTable$databaseId,
          cohortId = row$cohortId
        )
        return(data)
      })

    ## cohortConceptsetExpressionJson ---------------------------------------------------------
    output$cohortConceptsetExpressionJson <- shiny::renderText({
      if (is.null(cohortDefinitionConceptSetExpressionSelected())) {
        return(NULL)
      }
      json <- cohortDefinitionConceptSetExpressionSelected()$json
      return(json)
    })

    vocabSchema <- shiny::reactive({
      if (is.null(input$vocabularySelection)) {
        return("")
      }
      input$vocabularySelection
    })

    ### getCohortDefinitionMappedConceptsReactive ---------------------------------------------------------
    getCohortDefinitionMappedConceptsReactive <-
      shiny::reactive(x = {
        row <- selectedCohortDefinitionRow()
        if (is.null(row)) {
          return(NULL)
        }
        progress <- shiny::Progress$new()
        on.exit(progress$close())
        progress$set(message = "Getting concepts mapped to concept ids resolved by concept set expression (may take time)", value = 0)
        output <-
          mappedConceptSet(
            dataSource = dataSource,
            databaseIds = databaseTable$databaseId,
            cohortId = row$cohortId
          )

        if (!hasData(output)) {
          return(NULL)
        }
        conceptCount <- getCountForConceptIdInCohortReactive()
        output <- output %>%
          dplyr::left_join(conceptCount,
                           by = c("databaseId", "conceptId"))
        return(output)
      })

    output$cohortDefinitionMappedConceptsTable <-
      reactable::renderReactable(expr = {
        if (input$conceptSetsType != 'Mapped') {
          return(NULL)
        }

        databaseIdToFilter <- databaseTable %>%
          dplyr::filter(.data$databaseIdWithVocabularyVersion == vocabSchema()) %>%
          dplyr::pull("databaseId")
        if (!hasData(databaseIdToFilter)) {
          return(NULL)
        }

        shiny::validate(shiny::need(
          length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
          "Please select concept set"
        ))

        data <- getCohortDefinitionMappedConceptsReactive()
        shiny::validate(shiny::need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))

        data <- data %>%
          dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
          dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
          dplyr::rename("persons" = "conceptSubjects",
                        "records" = "conceptCount")
        shiny::validate(shiny::need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))

        keyColumns <- c(
          "resolvedConceptId",
          "conceptId",
          "conceptName",
          "domainId",
          "vocabularyId",
          "conceptClassId",
          "standardConcept",
          "conceptCode"
        )
        dataColumns <- c("persons",
                         "records")

        getDisplayTableSimple(data = data,
                              keyColumns = keyColumns,
                              dataColumns = dataColumns)

      })

    vocabularyChoices <- databaseTable$databaseIdWithVocabularyVersion
    names(vocabularyChoices) <- databaseTable$databaseName

    shiny::observe({
      shiny::updateSelectInput(session,
                               inputId = "vocabularySelection",
                               choices = vocabularyChoices)
    })

    ## Export all cohort details ----
    output$exportAllCohortDetails <- shiny::downloadHandler(
      filename = function() {
        paste("ExportDetails", "zip", sep = ".")
      },
      content = function(file) {
        shiny::withProgress(
          message = "Export is in progress",
        {
          definitions <- getCdCohortRows(dataSource, cohortTable$cohortId)
          exportCohortDefinitionsZip(definitions, zipFile = file)
        },
          detail = "Please Wait"
        )
      },
      contentType = "application/zip"
    )

  }

  shiny::moduleServer(id, cohortDefinitionServer)
}
