# Copyright 2022 Observational Health Data Sciences and Informatics
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

#' @param id            unqiue identifier for module. Must match call to annotationModule
annotationUi <- function(id) {
  ns <- shiny::NS(id)

  postAnnotationArea <- shiny::conditionalPanel(
        condition = "output.postAnnotationEnabled == true",
        ns = ns,
        shinydashboard::box(
          title = "Add comment",
          width = NULL,
          collapsible = TRUE,
          collapsed = TRUE,
          column(
            5,
            shinyWidgets::pickerInput(
              inputId = ns("database"),
              label = "Related Database:",
              width = 300,
              choices = c(""),
              selected = c(""),
              multiple = TRUE,
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
          ),
          column(
            5,
            shinyWidgets::pickerInput(
              inputId = ns("targetCohort"),
              label = "Related Cohorts",
              width = 300,
              choices = c(""),
              selected = c(""),
              multiple = TRUE,
              inline = TRUE,
              choicesOpt = list(style = rep_len("color: black;", 999)),
              options = shinyWidgets::pickerOptions(
                actionsBox = TRUE,
                liveSearch = TRUE,
                liveSearchStyle = "contains",
                size = 10,
                dropupAuto = TRUE,
                liveSearchPlaceholder = "Type here to search",
                virtualScroll = 50
              )
            )
          ),
          column(
            11,
            markdownInput::markdownInput(
              inputId = ns("markdownInputArea"),
              label = "Comment : ",
              theme = "github",
              value = "Write some _markdown_ **here:**"
            )
          ),
          column(
            1,
            tags$br(),
            shiny::actionButton(
              inputId = ns("postAnnotation"),
              label = "POST",
              width = NULL,
              style = "margin-top: 15px; margin-bottom: 15px;"
            )
          )
        )
      )

  return(
    shinydashboard::box(
      title = "Comments",
      width = NULL,
      collapsible = TRUE,
      collapsed = FALSE,
      reactable::reactableOutput(
        outputId = ns("comments"),
        width = NULL
      ),
      tags$style(
        paste0(
          "#output",
          id,
          " {max-height:300px;overflow:auto;padding-left:30px;margin:0 0 30px 10px;border-left:1px solid #eee;}"
        )
      ),
      postAnnotationArea
    )
  )
}

#' Annoation module
#' Adds annoation section that allows display and addition of markdown comments for cohorts
#'
#' @param id                        The namespace id of the module instance - must align with `annotationUi`
#' @param dataSource                Database intance used to store comments and retrieve them
#' @param activeLoggedInUser        shiny::reactive that returns the active logged in user that stores the comment
#' @param selectedDatabaseIds       shiny::reactive the current selected by the user
#' @param postAnnotaionEnabled     shiny::reactive - is posting enabled for the user?
#' @param multiCohortSelection      Boolean is the input set of cohorts many or one?
annotationModule <- function(id,
                             dataSource,
                             activeLoggedInUser,
                             selectedDatabaseIds,
                             selectedCohortIds,
                             cohortTable,
                             postAnnotaionEnabled) {
  ns <- shiny::NS(id)
  annotationServer <- function(input, output, session) {
    # Annotation Section ------------------------------------
    ## posting annotation enabled ------
    output$postAnnotationEnabled <- shiny::reactive({
      postAnnotaionEnabled() & !is.null(activeLoggedInUser())
    })
    outputOptions(output, "postAnnotationEnabled", suspendWhenHidden = FALSE)

    ## Retrieve Annotation ----------------
    reloadAnnotationSection <- reactiveVal(0)

    inputCohortIds <- shiny::reactive({
      cohortTable %>%
        dplyr::filter(.data$compoundName %in% selectedCohortIds()) %>%
        dplyr::pull(.data$cohortId)
    })

    getAnnotationReactive <- shiny::reactive({
      reloadAnnotationSection()
      results <- getAnnotationResult(
        dataSource = dataSource,
        diagnosticsId = id,
        cohortIds = inputCohortIds(),
        databaseIds = selectedDatabaseIds()
      )

      if (nrow(results$annotation) == 0) {
        return(NULL)
      }
      return(results)
    })

    markdownModule <- shiny::callModule(markdownInput::moduleMarkdownInput, "markdownInputArea")

    shiny::observe({
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "database",
        choices = selectedDatabaseIds(),
        selected = selectedDatabaseIds()
      )
    })

    shiny::observe({
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "targetCohort",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = selectedCohortIds(),
        selected = selectedCohortIds()
      )
    })

    ## renderedAnnotation ----
    output$comments <-
      reactable::renderReactable({
        results <- getAnnotationReactive()

        if (is.null(results)) {
          return(NULL)
        }
        data <- results$annotation
        for (i in 1:nrow(data)) {
          data[i,]$annotation <-
            markdown::renderMarkdown(text = data[i,]$annotation)
        }
        data <- data %>%
          dplyr::mutate(
            Annotation = paste0(
              "<b>",
              .data$createdBy,
              "@",
              getTimeFromInteger(.data$createdOn),
              ":</b>",
              .data$annotation
            )
          ) %>%
          dplyr::select(.data$annotationId, .data$Annotation)

        reactable::reactable(
          data,
          columns = list(
            annotationId = reactable::colDef(show = FALSE),
            Annotation = reactable::colDef(html = TRUE)
          ),
          details = function(index) {
            subTable <- results$annotationLink %>%
              dplyr::filter(.data$annotationId == data[index,]$annotationId) %>%
              dplyr::inner_join(cohortTable %>%
                                  dplyr::select(
                                    .data$cohortId,
                                    .data$cohortName
                                  ),
                                by = "cohortId"
              )
            distinctCohortName <- subTable %>%
              dplyr::distinct(.data$cohortName)
            distinctDatabaseId <- subTable %>%
              dplyr::distinct(.data$databaseId)

            htmltools::div(
              style = "margin:0;padding:0;padding-left:50px;",
              tags$p(
                style = "margin:0;padding:0;",
                "Related Cohorts: ",
                tags$p(
                  style = "padding-left:30px;",
                  tags$pre(
                    paste(distinctCohortName$cohortName, collapse = "\n")
                  )
                )
              ),
              tags$br(),
              tags$p(
                "Related Databses: ",
                tags$p(
                  style = "padding-left:30px;",
                  tags$pre(
                    paste(distinctDatabaseId$databaseId, collapse = "\n")
                  )
                )
              )
            )
          }
        )
      })


    ## Post Annotation ----------------
    getParametersToPostAnnotation <- shiny::reactive({
      tempList <- list()
      # Annotation - cohort Ids
      tempList$cohortIds <- inputCohortIds()

      # Annotation - database Ids
      if (!is.null(input$database)) {
        selectedDatabaseIds <- input$database
      } else {
        selectedDatabaseIds <- selectedDatabaseIds()
      }
      tempList$databaseIds <- selectedDatabaseIds
      return(tempList)
    })


    shiny::observeEvent(
      eventExpr = input$postAnnotation,
      handlerExpr = {
        parametersToPostAnnotation <- getParametersToPostAnnotation()
        comment <- markdownModule()

        if (comment == "Write some _markdown_ **here:**" | is.null(comment) | is.null(activeLoggedInUser())) {
          return(NULL)
        }
        createdBy <- activeLoggedInUser()
        result <- postAnnotationResult(
          dataSource = dataSource,
          diagnosticsId = id,
          cohortIds = parametersToPostAnnotation$cohortIds,
          databaseIds = parametersToPostAnnotation$databaseIds,
          annotation = comment,
          createdBy = createdBy,
          createdOn = getTimeAsInteger()
        )

        if (result) {
          # trigger reload
          reloadAnnotationSection(reloadAnnotationSection() + 1)
        }
      }
    )
  }

  return(shiny::moduleServer(id, annotationServer))
}


postAnnotationResult <- function(dataSource,
                                 diagnosticsId,
                                 cohortIds,
                                 databaseIds,
                                 annotation,
                                 createdBy,
                                 createdOn = getTimeAsInteger(),
                                 modifiedOn = NULL,
                                 deletedOn = NULL) {
  # Prevent potential sql injection
  annotation <- gsub("'", "`", annotation)
  sqlInsert <- "INSERT INTO @results_database_schema.annotation (
                                                          	annotation_id,
                                                          	created_by,
                                                          	created_on,
                                                          	modified_last_on,
                                                          	deleted_on,
                                                          	annotation
                                                          	)
                SELECT CASE
                		WHEN max(annotation_id) IS NULL
                			THEN 1
                		ELSE max(annotation_id) + 1
                		END AS annotation_id,
                	'@created_by' created_by,
                	@created_on created_on,
                	@modified_last_on modified_last_on,
                	@deleted_on deleted_on,
                	'@annotation' annotation
                FROM @results_database_schema.annotation;"

  tryCatch(
  {
    renderTranslateExecuteSql(
      dataSource = dataSource,
      sql = sqlInsert,
      results_database_schema = dataSource$resultsDatabaseSchema,
      annotation = annotation,
      created_by = createdBy,
      created_on = createdOn,
      modified_last_on = modifiedOn,
      deleted_on = deletedOn
    )
  },
    error = function(err) {
      stop(paste("Error while posting the comment, \nDescription:", err))
    }
  )

  # get annotation id
  sqlRetrieve <- "SELECT max(annotation_id) annotation_id
                  FROM @results_database_schema.annotation
                  WHERE created_by = '@created_by'
                  	AND created_on = @created_on;"
  maxAnnotationId <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlRetrieve,
      results_database_schema = dataSource$resultsDatabaseSchema,
      created_by = createdBy,
      created_on = createdOn
    ) %>% dplyr::pull()

  # insert annotation link
  annotationLink <-
    tidyr::crossing(
      annotationId = !!maxAnnotationId,
      diagnosticsId = !!diagnosticsId,
      cohortId = !!cohortIds,
      databaseId = !!databaseIds
    )
  realConnection <- pool::poolCheckout(dataSource$connection)
  DatabaseConnector::insertTable(
    connection = realConnection,
    databaseSchema = dataSource$resultsDatabaseSchema,
    tableName = "annotation_link",
    createTable = FALSE,
    dropTableIfExists = FALSE,
    tempTable = FALSE,
    progressBar = FALSE,
    camelCaseToSnakeCase = TRUE,
    data = annotationLink
  )
  pool::poolReturn(realConnection)
  return(TRUE)
}


getAnnotationResult <- function(dataSource,
                                diagnosticsId,
                                cohortIds,
                                databaseIds) {
  # get annotation id's
  sqlRetrieveAnnotationLink <- "SELECT *
                                FROM @results_database_schema.annotation_link
                                WHERE diagnostics_id = '@diagnosticsId'
                                	AND cohort_id IN (@cohortIds)
                                  AND database_id IN (@databaseIds);"
  annotationLink <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlRetrieveAnnotationLink,
      results_database_schema = dataSource$resultsDatabaseSchema,
      diagnosticsId = diagnosticsId,
      cohortIds = cohortIds,
      databaseIds = quoteLiterals(databaseIds),
      snakeCaseToCamelCase = TRUE
    )

  sqlRetrieveAnnotation <- "SELECT *
                            FROM @results_database_schema.annotation
                            WHERE annotation_id IN (@annotationIds);"

  annotation <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlRetrieveAnnotation,
      results_database_schema = dataSource$resultsDatabaseSchema,
      annotationIds = annotationLink$annotationId,
      snakeCaseToCamelCase = TRUE
    )

  data <- list(
    annotation = annotation,
    annotationLink = annotationLink
  )

  return(data)
}