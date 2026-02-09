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

#' Index event breakdown view
#' @description
#' Use for customizing UI
#'
#' @param id    Namespace Id - use namespaced id ns("indexEvents") inside diagnosticsExplorer module
#' @family CohortDiagnostics
#' @export
indexEventBreakdownView <- function(id) {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Index Events",
      width = "100%",
      shiny::htmlTemplate(file.path(cdWwwPath, "indexEventBreakdown.html"))
    ),
    shinydashboard::box(
      status = "warning",
      width = "100%",
      shiny::tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohort"))
      )
    ),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      shiny::withTags(
        table(
          width = "100%",
          shiny::tags$tr(
            shiny::tags$td(
              shiny::radioButtons(
                inputId = ns("indexEventBreakdownTableRadioButton"),
                label = "Concept type",
                choices = c("All", "Standard concepts", "Non Standard Concepts"),
                selected = "All",
                inline = TRUE
              )
            ),
            shiny::tags$td(shiny::HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
            shiny::tags$td(
              shiny::radioButtons(
                inputId = ns("indexEventBreakdownTableFilter"),
                label = "Display",
                choices = c("Both", "Records", "Persons"),
                selected = "Persons",
                inline = TRUE
              )
            ),
            shiny::tags$td(
              shiny::checkboxInput(
                inputId = ns("showAsPercent"),
                label = "Show persons as percentage",
                value = TRUE
              )
            )
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("breakdownTable"))),
      reactableCsvDownloadButton(ns, "breakdownTable")
    )
  )
}


getIndexEventBreakdown <- function(dataSource,
                                   cohortIds,
                                   cohortCount,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <-
    checkErrorCohortIdsDatabaseIds(
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      errorMessage = errorMessage
    )
  checkmate::reportAssertions(collection = errorMessage)

  sql <- "SELECT index_event_breakdown.*,
              concept.concept_name,
              concept.domain_id,
              concept.vocabulary_id,
              concept.standard_concept,
              concept.concept_code
            FROM  @schema.@table_name index_event_breakdown
            INNER JOIN  @vocabulary_database_schema.@concept_table concept
              ON index_event_breakdown.concept_id = concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
  data <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("index_event_breakdown"),
      concept_table = dataSource$prefixVocabTable("concept"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()


  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c("databaseId", "cohortId")
    ) %>%
    dplyr::mutate(
      subjectPercent = .data$subjectCount / .data$cohortSubjects,
      conceptPercent = .data$conceptCount / .data$cohortEntries
    )

  return(data)
}


indexEventBreakdownModule <- function(id,
                                      dataSource,
                                      selectedCohort,
                                      targetCohortId,
                                      selectedDatabaseIds,
                                      cohortCountTable = dataSource$cohortCountTable,
                                      databaseTable = dataSource$dbTable) {
  ns <- shiny::NS(id)

  serverFunction <- function(input, output, session) {

    output$selectedCohort <- shiny::renderUI(selectedCohort())

    # Index event breakdown -----------
    indexEventBreakDownData <- shiny::reactive(x = {
      if (length(targetCohortId()) > 0 &&
        length(selectedDatabaseIds()) > 0) {
        data <- getIndexEventBreakdown(
          dataSource = dataSource,
          cohortCount = cohortCountTable,
          cohortIds = targetCohortId(),
          databaseIds = selectedDatabaseIds()
        )
        if (any(
          is.null(data),
          nrow(data) == 0
        )) {
          return(NULL)
        }
        if (!is.null(data)) {
          if (!"domainTable" %in% colnames(data)) {
            data$domainTable <- "Not in data"
          }
          if (!"domainField" %in% colnames(data)) {
            data$domainField <- "Not in data"
          }
          return(data)
        } else {
          return(NULL)
        }
      } else {
        return(NULL)
      }
    })

    indexEventBreakDownDataFilteredByRadioButton <-
      shiny::reactive(x = {
        data <- indexEventBreakDownData()
        if (!is.null(data) && nrow(data) > 0) {
          if (input$indexEventBreakdownTableRadioButton == "All") {
            return(data)
          } else if (input$indexEventBreakdownTableRadioButton == "Standard concepts") {
            return(data %>% dplyr::filter(.data$standardConcept == "S"))
          } else {
            return(data %>% dplyr::filter(is.na(.data$standardConcept)))
          }
        } else {
          return(NULL)
        }
      })

    output$breakdownTable <- reactable::renderReactable(expr = {
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(length(targetCohortId()) > 0, "No cohorts chosen chosen"))

      showDataAsPercent <- input$showAsPercent
      data <- indexEventBreakDownDataFilteredByRadioButton()

      shiny::validate(shiny::need(
        all(!is.null(data), nrow(data) > 0),
        "There is no data for the selected combination."
      ))

      shiny::validate(shiny::need(
        nrow(data) > 0,
        "No data available for selected combination."
      ))

      data <- data %>%
        dplyr::arrange(.data$databaseId) %>%
        dplyr::select(
          "conceptId",
          "conceptName",
          "domainField",
          "databaseId",
          "vocabularyId",
          "conceptCode",
          "conceptCount",
          "subjectCount",
          "subjectPercent",
          "conceptPercent"
        ) %>%
        dplyr::filter(.data$conceptId > 0) %>%
        dplyr::distinct()

      if (showDataAsPercent) {
        data <- data %>%
          dplyr::rename(
            "persons" = "subjectPercent"
          )
      } else {
        data <- data %>%
          dplyr::rename(
            "persons" = "subjectCount"
          )
      }

      data <- data %>%
        dplyr::rename(
          "records" = "conceptCount"
        )

      data <- data %>%
        dplyr::arrange(dplyr::desc(abs(dplyr::across(
          c("records", "persons")
        ))))

      keyColumnFields <-
        c("conceptId", "conceptName", "conceptCode", "domainField", "vocabularyId")
      if (input$indexEventBreakdownTableFilter == "Persons") {
        dataColumnFields <- c("persons")
        countLocation <- 1
      } else if (input$indexEventBreakdownTableFilter == "Records") {
        dataColumnFields <- c("records")
        countLocation <- 1
      } else {
        dataColumnFields <- c("persons", "records")
        countLocation <- 2
      }

      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = targetCohortId(),
          source = "cohort",
          fields = input$indexEventBreakdownTableFilter
        )

      getDisplayTableGroupedByDatabaseId(
        data = data,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        showDataAsPercent = showDataAsPercent,
        excludedColumnFromPercentage = "records",
        sort = TRUE
      )
    })
  }


  return(shiny::moduleServer(id, serverFunction))
}
