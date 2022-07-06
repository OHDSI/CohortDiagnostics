#' Returns list with circe generated documentation
#'
#' @description
#' Returns list with circe generated documentation
#'
#' @param cohortDefinition An R object (list) with a list representation of the cohort definition expression,
#'                          that may be converted to a cohort expression JSON using
#'                          RJSONIO::toJSON(x = cohortDefinition, digits = 23, pretty = TRUE)
#'
#' @param cohortName Name for the cohort definition
#'
#' @param includeConceptSets Do you want to inclued concept set in the documentation
#'
#' @return list object
#'
#' @export
getCirceRenderedExpression <- function(cohortDefinition,
                                       cohortName = "Cohort Definition",
                                       includeConceptSets = FALSE) {
  cohortJson <-
    RJSONIO::toJSON(
      x = cohortDefinition,
      digits = 23,
      pretty = TRUE
    )
  circeExpression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
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
      cohortJson = cohortJson,
      cohortMarkdown = circeExpressionMarkdown,
      conceptSetMarkdown = circeConceptSetListmarkdown,
      cohortHtmlExpression = htmlExpressionCohort,
      conceptSetHtmlExpression = htmlExpressionConceptSetExpression
    )
  )
}


getConceptSetDataFrameFromConceptSetExpression <-
  function(conceptSetExpression) {
    if ("items" %in% names(conceptSetExpression)) {
      items <- conceptSetExpression$items
    } else {
      items <- conceptSetExpression
    }
    conceptSetExpressionDetails <- items %>%
      purrr::map_df(.f = purrr::flatten)
    if ("CONCEPT_ID" %in% colnames(conceptSetExpressionDetails)) {
      if ("isExcluded" %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(IS_EXCLUDED = .data$isExcluded)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(IS_EXCLUDED = FALSE)
      }
      if ("includeDescendants" %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_DESCENDANTS = .data$includeDescendants)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_DESCENDANTS = FALSE)
      }
      if ("includeMapped" %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_MAPPED = .data$includeMapped)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_MAPPED = FALSE)
      }
      conceptSetExpressionDetails <-
        conceptSetExpressionDetails %>%
        tidyr::replace_na(list(
          IS_EXCLUDED = FALSE,
          INCLUDE_DESCENDANTS = FALSE,
          INCLUDE_MAPPED = FALSE
        ))
      colnames(conceptSetExpressionDetails) <-
        SqlRender::snakeCaseToCamelCase(colnames(conceptSetExpressionDetails))
    }
    return(conceptSetExpressionDetails)
  }


getConceptSetDetailsFromCohortDefinition <-
  function(cohortDefinitionExpression) {
    if ("expression" %in% names(cohortDefinitionExpression)) {
      expression <- cohortDefinitionExpression$expression
    } else {
      expression <- cohortDefinitionExpression
    }

    if (is.null(expression$ConceptSets)) {
      return(NULL)
    }

    conceptSetExpression <- expression$ConceptSets %>%
      dplyr::bind_rows() %>%
      dplyr::mutate(json = RJSONIO::toJSON(
        x = .data$expression,
        pretty = TRUE
      ))

    conceptSetExpressionDetails <- list()
    i <- 0
    for (id in conceptSetExpression$id) {
      i <- i + 1
      conceptSetExpressionDetails[[i]] <-
        getConceptSetDataFrameFromConceptSetExpression(
          conceptSetExpression =
            conceptSetExpression[i, ]$expression$items
        ) %>%
        dplyr::mutate(id = conceptSetExpression[i, ]$id) %>%
        dplyr::relocate(.data$id) %>%
        dplyr::arrange(.data$id)
    }
    conceptSetExpressionDetails <-
      dplyr::bind_rows(conceptSetExpressionDetails)
    output <- list(
      conceptSetExpression = conceptSetExpression,
      conceptSetExpressionDetails = conceptSetExpressionDetails
    )
    return(output)
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
    cohortId <- cohort[i, ]$cohortId
    dir.create(
      path = file.path(tempdir, cohortId),
      recursive = TRUE,
      showWarnings = FALSE
    )
    cohortExpression <- cohortDefinitions[i, ]$json %>%
      RJSONIO::fromJSON(digits = 23)

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

  return(DatabaseConnector::createZipFile(zipFile = zipFile,
                                          files = tempdir,
                                          rootFolder = tempdir))
}

#' Cohort Definitions View
#' @description
#' Outputs cohort definitions
#'
#'
cohortDefinitionsView <- function(id) {
  ns <- shiny::NS(id)
  ui <- shiny::tagList(
    shinydashboard::box(
      width = NULL,
      status = "primary",
      htmltools::withTags(
        table(width = "100%",
              tr(
                td(align = "left",
                   h4("Cohort Definition")
                ),
                td(
                  align = "right",
                  shiny::downloadButton(
                    outputId = "exportAllCohortDetails",
                    label = "Export Cohorts Zip",
                    icon = shiny::icon("file-export"),
                    style = "margin-top: 5px; margin-bottom: 5px;"
                  )
                )
              )
        )
      ),
      shiny::column(12,
                    reactable::reactableOutput(outputId = ns("cohortDefinitionTable"))),
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
                            tags$br(),
                            reactable::reactableOutput(outputId = ns("cohortCountsTableInCohortDefinition"))),
            shiny::tabPanel(title = "Cohort definition",
                            copyToClipboardButton(toCopyId = ns("cohortDefinitionText"),
                                                  style = "margin-top: 5px; margin-bottom: 5px;"),
                            shiny::htmlOutput(ns("cohortDefinitionText"))),
            shiny::tabPanel(
              title = "Concept Sets",
              reactable::reactableOutput(outputId = ns("conceptsetExpressionsInCohort")),
              shiny::conditionalPanel(condition = "output.cohortDefinitionConceptSetExpressionRowIsSelected == true",
                                      ns = ns,
                                      tags$table(tags$tr(
                                        tags$td(
                                          shiny::radioButtons(
                                            inputId = ns("conceptSetsType"),
                                            label = "",
                                            choices = c("Concept Set Expression",
                                                        "Resolved",
                                                        "Mapped",
                                                        "Orphan concepts",
                                                        "Json"),
                                            selected = "Concept Set Expression",
                                            inline = TRUE
                                          )
                                        ),
                                        tags$td(
                                          shiny::uiOutput(ns("databasePicker"))
                                        ),
                                        tags$td(
                                          shiny::htmlOutput(ns("subjectCountInCohortConceptSet"))
                                        ),
                                        tags$td(
                                          shiny::htmlOutput(ns("recordCountInCohortConceptSet"))
                                        )
                                      ))),
              shiny::conditionalPanel(
                ns = ns,
                condition = "output.cohortDefinitionConceptSetExpressionRowIsSelected == true &
                  input.conceptSetsType != 'Resolved' &
                  input.conceptSetsType != 'Mapped' &
                  input.conceptSetsType != 'Json' &
                  input.conceptSetsType != 'Orphan concepts'",
                reactable::reactableOutput(outputId = ns("cohortDefinitionConceptSetDetailsTable"))
              ),
              shiny::conditionalPanel(
                ns = ns,
                condition = "input.conceptSetsType == 'Resolved'",
                reactable::reactableOutput(outputId = ns("cohortDefinitionResolvedConceptsTable"))
              ),
              shiny::conditionalPanel(
                ns = ns,
                condition = "input.conceptSetsType == 'Mapped'",
                reactable::reactableOutput(outputId = ns("cohortDefinitionMappedConceptsTable"))
              ),
              shiny::conditionalPanel(
                ns = ns,
                condition = "input.conceptSetsType == 'Orphan concepts'",
                reactable::reactableOutput(outputId = ns("cohortDefinitionOrphanConceptTable"))
              ),
              shiny::conditionalPanel(
                condition = "input.conceptSetsType == 'Json'",
                copyToClipboardButton(toCopyId = ns("cohortConceptsetExpressionJson"),
                                      style = "margin-top: 5px; margin-bottom: 5px;"),
                shiny::verbatimTextOutput(outputId = ns("cohortConceptsetExpressionJson")),
                tags$head(
                  tags$style("#cohortConceptsetExpressionJson { max-height:400px};")
                ),
                ns = ns
              )
            ),
            shiny::tabPanel(
              title = "JSON",
              copyToClipboardButton(ns("cohortDefinitionJson"), style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput(ns("cohortDefinitionJson")),
              tags$head(
                tags$style("#cohortDefinitionJson { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "SQL",
              copyToClipboardButton(ns("cohortDefinitionSql"), style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput(ns("cohortDefinitionSql")),
              tags$head(
                tags$style("#cohortDefinitionSql { max-height:400px};")
              )
            )
          )
        )
      )
    )
  )
  ui
}

#' Cohort Definition module
#' @description
#'
#'
#' @param id                            Namespace id
#' @param dataSource                    DatabaseConnection
#' @param cohortDefinitions             reactive of cohort definitions to display
#' @param databaseTable                 data.frame of databasese
cohortDefinitionsModule <- function(id,
                                    dataSource,
                                    cohortDefinitions,
                                    cohortTable,
                                    databaseTable) {
  ns <- shiny::NS(id)

  cohortDefinitionServer <- function(input, output, session) {

    cohortDefinitionTableData <- shiny::reactive(x = {
      data <- cohortDefinitions() %>%
        dplyr::select(cohort = .data$shortName, .data$cohortId, .data$cohortName)
      return(data)
    })

    # Cohort Definition ---------------------------------------------------------
    output$cohortDefinitionTable <-
      reactable::renderReactable(expr = {
        data <- cohortDefinitionTableData() %>%
          dplyr::mutate(cohortId = as.character(.data$cohortId))

        validate(need(hasData(data), "There is no data for this cohort."))
        keyColumns <- c("cohort", "cohortId", "cohortName")
        dataColumns <- c()

        displayTable <- getDisplayTableSimple(
          data = data,
          keyColumns = keyColumns,
          dataColumns = dataColumns,
          selection = "single"
        )
        return(displayTable)
      })

    selectedCohortDefinitionRow <- reactive({
      idx <- reactable::getReactableState("cohortDefinitionTable", "selected")
      if (is.null(idx)) {
        return(NULL)
      } else {
        subset <- cohortDefinitions()
        if (nrow(subset) == 0) {
          return(NULL)
        }
        row <- subset[idx[1],]
        return(row)
      }
    })

    output$cohortDefinitionRowIsSelected <- reactive({
      return(!is.null(selectedCohortDefinitionRow()))
    })

    outputOptions(output,
                  "cohortDefinitionRowIsSelected",
                  suspendWhenHidden = FALSE)

    ## cohortDetailsText ---------------------------------------------------------
    output$cohortDetailsText <- shiny::renderUI({
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      } else {
        tags$table(
          style = "margin-top: 5px;",
          tags$tr(
            tags$td(tags$strong("Cohort ID: ")),
            tags$td(HTML("&nbsp;&nbsp;")),
            tags$td(row$cohortId)
          ),
          tags$tr(
            tags$td(tags$strong("Cohort Name: ")),
            tags$td(HTML("&nbsp;&nbsp;")),
            tags$td(row$cohortName)
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
        data <- cohortCount
        if (!hasData(data)) {
          return(NULL)
        }
        data <- data %>%
          dplyr::filter(.data$cohortId == selectedCohortDefinitionRow()$cohortId) %>%
          dplyr::filter(.data$databaseId %in% databaseTable$databaseId) %>%
          dplyr::select(.data$databaseId,
                        .data$cohortSubjects,
                        .data$cohortEntries) %>%
          dplyr::rename("persons" = .data$cohortSubjects,
                        "events" = .data$cohortEntries)

        validate(need(hasData(data), "There is no data for this cohort."))

        keyColumns <- c("databaseId")
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
      details <-
        getCirceRenderedExpression(
          cohortDefinition = data$json[1] %>% RJSONIO::fromJSON(digits = 23),
          cohortName = data$cohortName[1],
          includeConceptSets = TRUE
        )
      return(details)
    })

    output$cohortDefinitionText <- shiny::renderUI(expr = {
      cohortDefinitionCirceRDetails()$cohortHtmlExpression %>%
        shiny::HTML()
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

      expression <- RJSONIO::fromJSON(row$json, digits = 23)
      if (is.null(expression)) {
        return(NULL)
      }
      expression <-
        getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = expression)

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
          dplyr::select(.data$id, .data$name)
      } else {
        return(NULL)
      }

      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no data for this cohort."
      ))

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
      return(is(dataSource, "environment"))
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
      data <- data %>%
        dplyr::filter(.data$id == cohortDefinitionConceptSetExpressionSelected()$id)
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::select(
          .data$conceptId,
          .data$conceptName,
          .data$isExcluded,
          .data$includeDescendants,
          .data$includeMapped,
          .data$standardConcept,
          .data$invalidReason,
          .data$conceptCode,
          .data$domainId,
          .data$vocabularyId,
          .data$conceptClassId
        )
      return(data)
    })

    output$cohortDefinitionConceptSetDetailsTable <-
      reactable::renderReactable(expr = {
        data <- cohortDefinitionConceptSetDetails()
        validate(need(
          all(!is.null(data),
              nrow(data) > 0),
          "There is no data for this cohort."
        ))
        if (is.null(cohortDefinitionConceptSetDetails())) {
          return(NULL)
        }

        data <- data %>%
          dplyr::rename(exclude = .data$isExcluded,
                        descendants = .data$includeDescendants,
                        mapped = .data$includeMapped,
                        invalid = .data$invalidReason)
        validate(need(
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
        data <- cohortCount %>%
          dplyr::filter(.data$cohortId == row$cohortId) %>%
          dplyr::filter(.data$databaseId == getDatabaseIdInCohortConceptSet()) %>%
          dplyr::select(.data$cohortSubjects, .data$cohortEntries)

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
        tags$table(
          tags$tr(
            tags$td("Subjects: "),
            tags$td(scales::comma(row$cohortSubjects, accuracy = 1))
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
        tags$table(
          tags$tr(
            tags$td("Records: "),
            tags$td(scales::comma(row$cohortEntries, accuracy = 1))
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
          dplyr::pull(.data$databaseId)
        if (!hasData(databaseIdToFilter)) {
          return(NULL)
        }

        validate(need(
          length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
          "Please select concept set"
        ))

        data <- getCohortDefinitionResolvedConceptsReactive()
        validate(need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))
        data <- data %>%
          dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
          dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
          dplyr::rename("subjects" = .data$conceptSubjects,
                        "count" = .data$conceptCount)
        validate(need(
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
        dataColumns <- c("subjects",
                         "count")
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

    ## Other ---------------------------------------------------------
    ### getConceptSetIds ---------------------------------------------------------
    getConceptSetIds <- shiny::reactive(x = {
      return(conceptSets$conceptSetId[conceptSets$conceptSetName %in% input$conceptSetsSelected])
    })

    ### getCohortDefinitionOrphanConceptsReactive ---------------------------------------------------------
    getCohortDefinitionOrphanConceptsReactive <- shiny::reactive(x = {
      validate(need(
        all(
          !is.null(getDatabaseIdInCohortConceptSet()),
          length(getDatabaseIdInCohortConceptSet()) > 0
        ),
        "Orphan codes are not available for reference vocabulary in this version."
      ))
      if (is.null(row) ||
        length(cohortDefinitionConceptSetExpressionSelected()$name) == 0) {
        return(NULL)
      }
      validate(need(
        length(input$vocabularySchema) > 0,
        "No data sources chosen"
      ))
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      }
      output <- getOrphanConceptResult(
        dataSource = dataSource,
        databaseIds = databaseTable$databaseId,
        cohortId = row$cohortId
      )
      if (!hasData(output)) {
        return(NULL)
      }
      output <- output %>%
        dplyr::anti_join(getCohortDefinitionResolvedConceptsReactive() %>%
                           dplyr::select(.data$conceptId) %>%
                           dplyr::distinct(),
                         by = "conceptId")
      if (!hasData(output)) {
        return(NULL)
      }
      output <- output %>%
        dplyr::anti_join(getCohortDefinitionMappedConceptsReactive() %>%
                           dplyr::select(.data$conceptId) %>%
                           dplyr::distinct(),
                         by = "conceptId")
      if (!hasData(output)) {
        return(NULL)
      }
      output <- output %>%
        dplyr::rename("persons" = .data$conceptSubjects,
                      "records" = .data$conceptCount)
      return(output)
    })

    vocabSchema <- shiny::reactive({
      browser()
      if (is.null(input$vocabularySchema)) {
        return("")
      }
      input$vocabularySchema
    })

    output$cohortDefinitionOrphanConceptTable <-
      reactable::renderReactable(expr = {
        if (input$conceptSetsType != 'Orphan concepts') {
          return(NULL)
        }

        databaseIdToFilter <- databaseTable %>%
          dplyr::filter(.data$databaseIdWithVocabularyVersion == vocabSchema()) %>%
          dplyr::pull(.data$databaseId)
        if (!hasData(databaseIdToFilter)) {
          return(NULL)
        }
        data <- getCohortDefinitionOrphanConceptsReactive()
        validate(need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))
        data <- data %>%
          dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
          dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
          dplyr::rename(
            "subjects" = .data$persons,
            "count" = .data$records,
            "standard" = .data$standardConcept
          )
        validate(need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))
        keyColumns <-
          c("conceptId",
            "conceptName",
            "vocabularyId",
            "conceptCode",
            "standard")
        dataColumns <- c("subjects",
                         "count")

        displayTable <- getDisplayTableSimple(data = data,
                                              keyColumns = keyColumns,
                                              dataColumns = dataColumns)
        return(displayTable)
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
          dplyr::pull(.data$databaseId)
        if (!hasData(databaseIdToFilter)) {
          return(NULL)
        }

        validate(need(
          length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
          "Please select concept set"
        ))

        data <- getCohortDefinitionMappedConceptsReactive()
        validate(need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))

        data <- data %>%
          dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
          dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
          dplyr::rename("subjects" = .data$conceptSubjects,
                        "count" = .data$conceptCount)
        validate(need(
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
        dataColumns <- c("subjects",
                         "count")

        getDisplayTableSimple(data = data,
                              keyColumns = keyColumns,
                              dataColumns = dataColumns)

      })


    vocabularyChoices <- list(
      'From site' = databaseTable$databaseIdWithVocabularyVersion,
      'Reference Vocabulary' = dataSource$vocabularyDatabaseSchema
    )

    output$databasePicker <- shiny::renderUI({
      shinyWidgets::pickerInput(
        inputId = ns("vocabularySchema"),
        label = "Vocabulary version:",
        choices = vocabularyChoices,
        multiple = FALSE,
        width = 200,
        inline = TRUE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          size = 10,
          liveSearchStyle = "contains",
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
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
          exportCohortDefinitionsZip(
            cohortDefintions = cohortTable,
            zipFile = file
          )
        },
          detail = "Please Wait"
        )
      },
      contentType = "application/zip"
    )

  }

  shiny::moduleServer(id, cohortDefinitionServer)
}
