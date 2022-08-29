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
    if (hasData(conceptSetExpression)) {
      conceptSetExpression <- conceptSetExpression %>% 
        dplyr::arrange(.data$id) 
    }
    
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
    if (hasData(conceptSetExpressionDetails)) {
      conceptSetExpressionDetails <- conceptSetExpressionDetails %>% 
        dplyr::arrange(.data$id,
                       .data$conceptId) 
    }
    
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
                                        tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
                                        tags$td(
                                          shiny::radioButtons(
                                            inputId = ns("conceptSetValueTypeFilter"),
                                            label = "Display",
                                            choices = c("Both", "Persons", "Records"),
                                            selected = "Persons",
                                            inline = TRUE
                                          )
                                        )
                                      ))
                                      
                                      ),
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
            ),
            shiny::tabPanel(title = "Cohort Metadata",
                            shiny::htmlOutput(ns("cohortDetailsText")))
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
#' @param databaseTable                 data.frame of database
#' @param selectedDatabaseIds           selected data sources
cohortDefinitionsModule <- function(id,
                                    dataSource,
                                    cohortDefinitions,
                                    cohortTable,
                                    selectedDatabaseIds,
                                    databaseTable) {
  ns <- shiny::NS(id)

  cohortDefinitionServer <- function(input, output, session) {

    cohortDefinitionTableData <- shiny::reactive(x = {
      data <- cohortDefinitions() %>%
        dplyr::select(.data$cohortId, .data$cohortName) %>% 
        dplyr::arrange(.data$cohortId)
      return(data)
    })

    # Cohort Definition ---------------------------------------------------------
    output$cohortDefinitionTable <-
      reactable::renderReactable(expr = {
        data <- cohortDefinitionTableData()

        validate(need(hasData(data), "There is no data for this cohort."))
        keyColumns <- c("cohortId", "cohortName")
        dataColumns <- c()
        
        displayTable <- getDisplayTableSimple(
          data = data,
          databaseTable = databaseTable,
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
        
        if (!hasData(selectedDatabaseIds())) {
          return(NULL)
        }
        
        data <- data %>%
          dplyr::filter(.data$cohortId == selectedCohortDefinitionRow()$cohortId) %>%
          dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
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
      if (!hasData(data)) {
        return(NULL)
      }
      if (hasData(data$conceptSetExpression)) {
        data <- data$conceptSetExpression %>%
          dplyr::select(.data$id, .data$name) %>% 
          dplyr::arrange(.data$id, .data$name)
      } else {
        return(NULL)
      }

      validate(need(hasData(data),
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
        if (hasData(data)) {
          return(data)
        } else {
          return(NULL)
        }
      }
    })

    ### output cohortDefinitionConceptSetExpressionRowIsSelected----
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

    ### cohortConceptsetExpressionJson ---------------------------------------------------------
    output$cohortConceptsetExpressionJson <- shiny::renderText({
      if (is.null(cohortDefinitionConceptSetExpressionSelected())) {
        return(NULL)
      }
      json <- cohortDefinitionConceptSetExpressionSelected()$json
      return(json)
    })
    
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

    ## Resolved Concepts ----
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
        return(output)
      })
    
    
    ### getCohortDefinitionResolvedConceptsReactiveFiltered ---------------------------------------------------------
    getCohortDefinitionResolvedConceptsReactiveFiltered <-
      shiny::reactive(x = {
        output <- getCohortDefinitionResolvedConceptsReactive()
        if (!hasData(output)) {
          return(NULL)
        }
        if (!hasData(selectedDatabaseIds())) {
          return(NULL)
        }
        output <- output %>% 
          dplyr::filter(.data$cohortId %in% c(selectedCohortDefinitionRow()$cohortId),
                        .data$conceptSetId %in% c(cohortDefinitionConceptSetExpressionSelected()$id),
                        .data$databaseId %in% c(selectedDatabaseIds()))
        
        if (!hasData(output)) {
          return(NULL)
        }
        allConceptIds <- output %>% 
          dplyr::select(.data$cohortId,
                        .data$conceptSetId,
                        .data$conceptId,
                        .data$conceptName,
                        .data$domainId,
                        .data$vocabularyId,
                        .data$conceptClassId,
                        .data$standardConcept,
                        .data$conceptCode) %>% 
          dplyr::distinct() 
        
        allConceptIdsAllDatabase <- allConceptIds %>% 
          tidyr::crossing(databaseTable %>% 
                            dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds())) %>% 
                            dplyr::select(.data$databaseId))
        
        conceptCount <- getCountForConceptIdInCohortReactive()
        
        output <- allConceptIdsAllDatabase %>%
          dplyr::left_join(
            conceptCount %>%
              dplyr::rename(
                "persons" = .data$conceptSubjects,
                "records" = .data$conceptCount
              ),
            by = c("databaseId", "conceptId")
          )
        return(output)
      })

    ### output - cohortDefinitionResolvedConceptsTable ----
    output$cohortDefinitionResolvedConceptsTable <-
      reactable::renderReactable(expr = {
        if (input$conceptSetsType != 'Resolved') {
          return(NULL)
        }
        
        validate(need(
          length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
          "Please select concept set"
        ))
        data <- getCohortDefinitionResolvedConceptsReactiveFiltered()
        
        validate(need(
          hasData(data),
          paste0("No resolved concept id for any of the data sources")
        ))
        
        keyColumnFields <- c(
          "conceptId",
          "conceptName",
          "standardConcept"
        )
        
        if (input$conceptSetValueTypeFilter == "Records") {
          dataColumnFields <- c("records")
          countLocation <- 1
        } else if (input$conceptSetValueTypeFilter == "Persons") {
          dataColumnFields <- c("persons")
          countLocation <- 1
        } else {
          dataColumnFields <- c("persons", "records")
          countLocation <- 2
        }
        
        fields <- input$conceptSetValueTypeFilter
        
        countsForHeader <-
          getDisplayTableHeaderCount(
            dataSource = dataSource,
            databaseIds = selectedDatabaseIds(),
            cohortIds = selectedCohortDefinitionRow()$cohortId,
            source = "cohort",
            fields = fields
          )
        
        maxCountValue <-
          getColumnMax(
            data = data,
            string = dataColumnFields
          )
        
        displayTable <- getDisplayTableGroupedByDatabaseId(
          data = data,
          cohort = cohortTable,
          databaseTable = databaseTable,
          headerCount = countsForHeader,
          keyColumns = keyColumnFields,
          countLocation = countLocation,
          dataColumns = dataColumnFields,
          maxCount = maxCountValue,
          sort = TRUE,
          selection = "single"
        )
        return(displayTable)
      })


    ## Orphan Concepts ---------------------------------------------------------
    ### getCohortDefinitionOrphanConceptsReactive ---------------------------------------------------------
    getCohortDefinitionOrphanConceptsReactive <-
      shiny::reactive(x = {
        row <- selectedCohortDefinitionRow()
        if (!hasData(row)) {
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
        return(output)
      })
    
    
    ### getCohortDefinitionOrphanConceptsReactiveFiltered ---------------------------------------------------------
    getCohortDefinitionOrphanConceptsReactiveFiltered <- shiny::reactive(x = {
      output <- getCohortDefinitionOrphanConceptsReactive()
      if (!hasData(output)) {
        return(NULL)
      }
      
      if (!hasData(selectedDatabaseIds())) {
        return(NULL)
      }
      output <- output %>% 
        dplyr::filter(.data$cohortId %in% c(selectedCohortDefinitionRow()$cohortId),
                      .data$conceptSetId %in% c(cohortDefinitionConceptSetExpressionSelected()$id),
                      .data$databaseId %in% c(selectedDatabaseIds()))
      
      # remove concepts that are portential orphans but are in resolved concepts
      output <- output %>%
        dplyr::anti_join(getCohortDefinitionResolvedConceptsReactiveFiltered() %>%
                           dplyr::select(.data$conceptId) %>%
                           dplyr::distinct(),
                         by = "conceptId")
      
      if (!hasData(output)) {
        return(NULL)
      }
      
      allConceptIds <- output %>% 
        dplyr::select(.data$cohortId,
                      .data$conceptSetId,
                      .data$conceptId,
                      .data$conceptName,
                      .data$vocabularyId,
                      .data$standardConcept,
                      .data$conceptCode) %>% 
        dplyr::distinct() 
      
      allConceptIdsAllDatabase <- allConceptIds %>% 
        tidyr::crossing(databaseTable %>% 
                          dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds())) %>% 
                          dplyr::select(.data$databaseId))
      
      conceptCount <- getCountForConceptIdInCohortReactive()
      
      output <- allConceptIdsAllDatabase %>%
        dplyr::left_join(
          conceptCount %>%
            dplyr::rename(
              "persons" = .data$conceptSubjects,
              "records" = .data$conceptCount
            ),
          by = c("databaseId", "conceptId")
        )
      return(output)
    })

    ### output - cohortDefinitionOrphanConceptTable ----
    output$cohortDefinitionOrphanConceptTable <-
      reactable::renderReactable(expr = {
        if (input$conceptSetsType != 'Orphan concepts') {
          return(NULL)
        }

        data <- getCohortDefinitionOrphanConceptsReactiveFiltered()
        validate(need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))
        
        keyColumnFields <- c(
          "conceptId",
          "conceptName",
          "standardConcept",
          "vocabularyId",
          "conceptCode"
        )
        
        if (input$conceptSetValueTypeFilter == "Records") {
          dataColumnFields <- c("records")
          countLocation <- 1
        } else if (input$conceptSetValueTypeFilter == "Persons") {
          dataColumnFields <- c("persons")
          countLocation <- 1
        } else {
          dataColumnFields <- c("persons", "records")
          countLocation <- 2
        }
        
        fields <- input$conceptSetValueTypeFilter
        
        countsForHeader <-
          getDisplayTableHeaderCount(
            dataSource = dataSource,
            databaseIds = selectedDatabaseIds(),
            cohortIds = selectedCohortDefinitionRow()$cohortId,
            source = "cohort",
            fields = fields
          )
        
        maxCountValue <-
          getColumnMax(
            data = data,
            string = dataColumnFields
          )
        
        displayTable <- getDisplayTableGroupedByDatabaseId(
          data = data,
          cohort = cohortTable,
          databaseTable = databaseTable,
          headerCount = countsForHeader,
          keyColumns = keyColumnFields,
          countLocation = countLocation,
          dataColumns = dataColumnFields,
          maxCount = maxCountValue,
          sort = TRUE,
          selection = "single"
        )
        return(displayTable)
      })

    ## Mapped Concepts ---------------------------------------------------------
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
            cohortId = row$cohortId,
            conceptSetId = cohortDefinitionConceptSetExpressionSelected()$id
          )
        if (!hasData(output)) {
          return(NULL)
        }
        return(output)
      })
    
    ### getCohortDefinitionMappedConceptsReactiveFiltered ---------------------------------------------------------
    getCohortDefinitionMappedConceptsReactiveFiltered <-
      shiny::reactive(x = {
        output <- getCohortDefinitionMappedConceptsReactive()
        if (!hasData(output)) {
          return(NULL)
        }
        allConceptIdsAllDatabase <- output %>%
          tidyr::crossing(
            databaseTable %>%
              dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds())) %>%
              dplyr::select(.data$databaseId)
          )
        conceptCount <- getCountForConceptIdInCohortReactive()
        if (hasData(conceptCount)) {
          allConceptIdsAllDatabase <- allConceptIdsAllDatabase %>%
            dplyr::left_join(
              conceptCount %>%
                dplyr::rename(
                  "persons" = .data$conceptSubjects,
                  "records" = .data$conceptCount
                ),
              by = c("databaseId", "conceptId")
            )
        }
        return(allConceptIdsAllDatabase)
      })
    
    ### output - cohortDefinitionMappedConceptsTable ----
    output$cohortDefinitionMappedConceptsTable <-
      reactable::renderReactable(expr = {
        if (input$conceptSetsType != 'Mapped') {
          return(NULL)
        }
        validate(need(
          length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
          "Please select concept set"
        ))

        data <- getCohortDefinitionMappedConceptsReactiveFiltered()
        validate(need(
          hasData(data),
          paste0("No data for database id ", input$vocabularySchema)
        ))

        keyColumnFields <- c(
          "resolvedConceptId",
          "conceptId",
          "conceptName",
          "standardConcept",
          "vocabularyId",
          "conceptCode"
        )
        
        if (input$conceptSetValueTypeFilter == "Records") {
          dataColumnFields <- c("records")
          countLocation <- 1
        } else if (input$conceptSetValueTypeFilter == "Persons") {
          dataColumnFields <- c("persons")
          countLocation <- 1
        } else {
          dataColumnFields <- c("persons", "records")
          countLocation <- 2
        }
        
        fields <- input$conceptSetValueTypeFilter
        
        countsForHeader <-
          getDisplayTableHeaderCount(
            dataSource = dataSource,
            databaseIds = selectedDatabaseIds(),
            cohortIds = selectedCohortDefinitionRow()$cohortId,
            source = "cohort",
            fields = fields
          )
        
        maxCountValue <-
          getColumnMax(
            data = data,
            string = dataColumnFields
          )
        
        displayTable <- getDisplayTableGroupedByDatabaseId(
          data = data,
          cohort = cohortTable,
          databaseTable = databaseTable,
          headerCount = countsForHeader,
          keyColumns = keyColumnFields,
          countLocation = countLocation,
          dataColumns = dataColumnFields,
          maxCount = maxCountValue,
          sort = TRUE,
          selection = "single"
        )
        return(displayTable)

      })


    ## Other ----
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
