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

#' Cohort Counts View
#' @description
#' Shiny view for cohort counts module
#' @param id            Namespace id
#' @family CohortDiagnostics
#' @export
cohortCountsView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Cohort Counts",
      width = "100%",
      shiny::htmlTemplate(system.file("cohort-diagnostics-www", "cohortCounts.html", package = utils::packageName()))
    ),
    shinydashboard::box(
      status = "warning",
      width = "100%",
      shiny::tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohorts"))
      )
    ),
    shinydashboard::box(
      width = "100%",
      shiny::tagList(
        shiny::tags$table(
          shiny::tags$tr(
            shiny::tags$td(
              shiny::radioButtons(
                inputId = ns("cohortCountsTableColumnFilter"),
                label = "Display",
                choices = c("Both", "Persons", "Records"),
                selected = "Both",
                inline = TRUE
              )
            )
          )
        ),
        shinycssloaders::withSpinner(
          reactable::reactableOutput(outputId = ns("cohortCountsTable")
          )
        ),
        reactableCsvDownloadButton(ns, "cohortCountsTable"),
        shiny::conditionalPanel(
          condition = "output.cohortCountRowIsSelected == true",
          ns = ns,
          shiny::tags$h4("Inclusion Rule Statistics"),

          shiny::fluidRow(
            shiny::column(
              width = 4,
              shiny::radioButtons(
                inputId = ns("cohortCountInclusionRuleTableFilters"),
                label = "Inclusion Rule Events",
                choices = c("All", "Meet", "Gain", "Remain"),
                selected = "All",
                inline = TRUE
              )
            ),
            shiny::column(
              width = 4,
              shiny::radioButtons(
                inputId = ns("showPersonOrEvents"),
                label = "Report",
                choices = c("Persons", "Events"),
                selected = "Persons",
                inline = TRUE
              )
            ),
            shiny::column(
              width = 4,
              shiny::checkboxInput(
                inputId = ns("showAsPercent"),
                label = "Show as percent",
                value = TRUE
              )
            )
          ),
          shinycssloaders::withSpinner(
            reactable::reactableOutput(ns("inclusionRuleStats"))
          ),
          reactableCsvDownloadButton(ns, "inclusionRuleStats")
        )
      )
    )
  )
}

getInclusionRulesTable <- function(
    dataSource, 
    cohortIds, 
    databaseIds, 
    dataColumnFields, 
    mode, 
    showAsPercentage
    ) {

  data <- getInclusionRuleStats(
    dataSource = dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    modeId = mode # modeId = 1 - best event, i.e. person
  )

  shiny::validate(shiny::need(
    (nrow(data) > 0),
    "There is no data for the selected combination."
  ))

  if (all(hasData(showAsPercentage), showAsPercentage)) {
    data <- data %>%
      dplyr::mutate(
        Meet = .data$meetSubjects / .data$totalSubjects,
        Gain = .data$gainSubjects / .data$totalSubjects,
        Remain = .data$remainSubjects / .data$totalSubjects,
        id = .data$ruleSequenceId
      )
  } else {
    data <- data %>%
      dplyr::mutate(
        Meet = .data$meetSubjects,
        Gain = .data$gainSubjects,
        Remain = .data$remainSubjects,
        Total = .data$totalSubjects,
        id = .data$ruleSequenceId
      )
  }

  data <- data %>%
    dplyr::arrange(
      .data$cohortId,
      .data$databaseId,
      .data$id
    )

  shiny::validate(shiny::need(
    (nrow(data) > 0),
    "There is no data for the selected combination."
  ))

  if (all(hasData(showAsPercentage), !showAsPercentage)) {
    dataColumnFields <- c(dataColumnFields, "Total")
  }

  countsForHeader <-
    getDisplayTableHeaderCount(
      dataSource = dataSource,
      databaseIds = databaseIds,
      cohortIds = cohortIds,
      source = "cohort",
      fields = "Persons"
    )

  getDisplayTableGroupedByDatabaseId(
    data = data,
    databaseTable = dataSource$dbTable,
    headerCount = countsForHeader,
    keyColumns = c("id", "ruleName"),
    countLocation = 1,
    dataColumns = dataColumnFields,
    showDataAsPercent = showAsPercentage,
    sort = TRUE
  )
}

#' Shiny module for cohort counts
#' @description
#' Shiny module for cohort counts. Displays reactable table of cohort counts
#'
#' @param id                        namespace id
#' @param dataSource                Backend Data source (DatabaseConnection)
#' @param cohortTable               data.frame of all cohorts
#' @param databaseTable             data.frame of all databases
#' @param selectedCohorts           shiny::reactive - should return cohorts selected or NULL
#' @param selectedDatabaseIds       shiny::reactive - should return cohorts selected or NULL
#' @param cohortIds                 shiny::reactive - should return cohorts selected integers or NULL
#' @family CohortDiagnostics
cohortCountsModule <- function(id,
                               dataSource,
                               cohortTable = dataSource$cohortTable,
                               databaseTable = dataSource$dbTable,
                               selectedCohorts,
                               selectedDatabaseIds,
                               cohortIds) {
  ns <- shiny::NS(id)

  serverFunction <- function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI(selectedCohorts())

    # Cohort Counts ----------------------
    getResults <- shiny::reactive(x = {
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(length(cohortIds()) > 0, "No cohorts chosen"))
      data <- getResultsCohortCounts(
        dataSource = dataSource,
        databaseIds = selectedDatabaseIds(),
        cohortIds = cohortIds()
      )
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data %>%
        dplyr::inner_join(cohortTable %>% dplyr::select("cohortName", "cohortId"), by = "cohortId") %>%
        dplyr::arrange(.data$cohortId, .data$databaseId)

      return(data)
    })

    output$cohortCountsTable <- reactable::renderReactable(expr = {
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(length(cohortIds()) > 0, "No cohorts chosen"))

      data <- getResults()
      shiny::validate(shiny::need(hasData(data), "There is no data on any cohort"))

      data <- getResults() %>%
        dplyr::rename(
          "persons" = "cohortSubjects",
          "records" = "cohortEntries"
        )

      dataColumnFields <- c("persons", "records")
      if (input$cohortCountsTableColumnFilter == "Persons") {
        dataColumnFields <- "persons"
      } else if (input$cohortCountsTableColumnFilter == "Records") {
        dataColumnFields <- "records"
      }

      keyColumnFields <- c("cohortId", "cohortName")

      countsForHeader <- NULL

      displayTable <- getDisplayTableGroupedByDatabaseId(
        data = data,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        sort = FALSE,
        selection = "single"
      )
      return(displayTable)
    })

    getCohortIdOnCohortCountRowSelect <- shiny::reactive({
      idx <- reactable::getReactableState(outputId = "cohortCountsTable", "selected")

      if (!hasData(idx)) {
        return(NULL)
      } else {
        if (hasData(getResults())) {
          subset <- getResults() %>%
            dplyr::select(
              "cohortId"
            ) %>%
            dplyr::distinct()
          subset <- subset[idx,]
          return(subset)
        } else {
          return(NULL)
        }
      }
    })

    output$cohortCountRowIsSelected <- shiny::reactive({
      return(!is.null(getCohortIdOnCohortCountRowSelect()))
    })

    # Check note: where is this function from?
    shiny::outputOptions(output,
                  "cohortCountRowIsSelected",
                  suspendWhenHidden = FALSE)

    output$inclusionRuleStats <- reactable::renderReactable({
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(
        nrow(getCohortIdOnCohortCountRowSelect()) > 0,
        "No cohorts chosen"
      ))

      if (!hasData(getCohortIdOnCohortCountRowSelect())) {
        return(NULL)
      }
      if (any(
        !hasData(input$showPersonOrEvents),
        input$showPersonOrEvents == "Persons"
      )) {
        mode <- 1
      } else {
        mode <- 0
      }

      if (any(!hasData(input$cohortCountInclusionRuleTableFilters),
              input$cohortCountInclusionRuleTableFilters == "All")) {
        dataColumnFields <- c("Meet", "Gain", "Remain")
      } else {
        dataColumnFields <- c(input$cohortCountInclusionRuleTableFilters)
      }

      getInclusionRulesTable(dataSource,
                             getCohortIdOnCohortCountRowSelect()$cohortId,
                             selectedDatabaseIds(),
                             dataColumnFields,
                             mode,
                             input$showAsPercent)
    })
  }

  return(shiny::moduleServer(id, serverFunction))
}
