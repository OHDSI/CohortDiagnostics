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

#' inclusion Rules View
#' @description
#' Use for customizing UI
#'
#' @param id    Namespace Id - use namespaced id ns("inclusionRules") inside diagnosticsExplorer module
#' @family CohortDiagnostics
#' @export
inclusionRulesView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
     shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Inclusion Rules",
      width = "100%",
      shiny::htmlTemplate(system.file("cohort-diagnostics-www",  "inclusionRuleStats.html", package = utils::packageName()))
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
      title = NULL,
      width = NULL,
      shiny::withTags(
        table(
          width = "100%",
          shiny::tags$tr(  # TODO where is this from?
            shiny::tags$td( # TODO where is this from?
              align = "left",
              shiny::radioButtons(
                inputId = ns("inclusionRuleTableFilters"),
                label = "Inclusion Rule Events",
                choices = c("All", "Meet", "Gain", "Remain"),
                selected = "All",
                inline = TRUE
              )
            ),
            shiny::tags$td(
              shiny::checkboxInput(
                inputId = ns("inclusionRulesShowAsPercent"),
                label = "Show as percent",
                value = TRUE
              )
            ),
            shiny::tags$td(
              align = "right",
            )
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("inclusionRuleTable"))),
      reactableCsvDownloadButton(ns, "inclusionRuleTable")
    )
  )
}

# inclusion Rules Module
inclusionRulesModule <- function(id,
                                 dataSource,
                                 databaseTable = dataSource$dbTable,
                                 selectedCohort,
                                 targetCohortId,
                                 selectedDatabaseIds) {

  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohort <- shiny::renderUI(selectedCohort())

    # Inclusion rules table ------------------
    output$inclusionRuleTable <- reactable::renderReactable(expr = {
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      table <- getInclusionRuleStats(
        dataSource = dataSource,
        cohortIds = targetCohortId(),
        databaseIds = selectedDatabaseIds(),
        modeId = 0
      )
      shiny::validate(shiny::need(hasData(table), "There is no data for the selected combination."))

      showDataAsPercent <- input$inclusionRulesShowAsPercent

      if (showDataAsPercent) {
        table <- table %>%
          dplyr::mutate(
            Meet = .data$meetSubjects / .data$totalSubjects,
            Gain = .data$gainSubjects / .data$totalSubjects,
            Remain = .data$remainSubjects / .data$totalSubjects,
            id = .data$ruleSequenceId
          )
      } else {
        table <- table %>%
          dplyr::mutate(
            Meet = .data$meetSubjects,
            Gain = .data$gainSubjects,
            Remain = .data$remainSubjects,
            Total = .data$totalSubjects,
            id = .data$ruleSequenceId
          )
      }

      table <- table %>%
        dplyr::arrange(.data$cohortId,
                       .data$databaseId,
                       .data$id)

      shiny::validate(shiny::need(
        (nrow(table) > 0),
        "There is no data for the selected combination."
      ))

      keyColumnFields <-
        c("id", "ruleName")
      countLocation <- 1
      
      if (any(!hasData(input$inclusionRuleTableFilters),
              input$inclusionRuleTableFilters == "All")) {
        dataColumnFields <- c("Meet", "Gain", "Remain")
      } else {
        dataColumnFields <- c(input$inclusionRuleTableFilters)
      }

      if (all(hasData(showDataAsPercent), !showDataAsPercent)) {
        dataColumnFields <- c(dataColumnFields, "Total")
      }

      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = targetCohortId(),
          source = "cohort",
          fields = "Persons"
        )

      getDisplayTableGroupedByDatabaseId(
        data = table,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        showDataAsPercent = showDataAsPercent,
        sort = FALSE
      )
    })
  })
}
