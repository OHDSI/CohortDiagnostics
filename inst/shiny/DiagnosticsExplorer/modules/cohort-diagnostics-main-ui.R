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

cdUiControls <- function(ns) {
  panels <- shiny::tagList(
    shiny::conditionalPanel(
      condition = "
      input.tabs == 'databaseInformation'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("database"),
        label = "Database",
        choices = NULL,
        multiple = FALSE,
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
    shiny::conditionalPanel(
      condition = "input.tabs=='incidenceRates' |
      input.tabs == 'timeDistribution' |
      input.tabs == 'cohortCounts' |
      input.tabs == 'indexEvents' |
      input.tabs == 'conceptsInDataSource' |
      input.tabs == 'orphanConcepts' |
      input.tabs == 'inclusionRules' |
      input.tabs == 'visitContext' |
      input.tabs == 'cohortOverlap'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("databases"),
        label = "Database(s)",
        choices = NULL,
        multiple = TRUE,
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
    shiny::conditionalPanel(
      condition = "
      input.tabs == 'conceptsInDataSource' |
      input.tabs == 'orphanConcepts'|
      input.tabs == 'inclusionRules'|
      input.tabs == 'indexEvents' |
      input.tabs == 'visitContext'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("targetCohort"),
        label = "Cohort",
        choices = c(""),
        multiple = FALSE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          liveSearchStyle = "contains",
          size = 10,
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs == 'cohortCounts' |
      input.tabs == 'cohortOverlap' |
      input.tabs == 'incidenceRates' |
      input.tabs == 'timeDistribution'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("cohorts"),
        label = "Cohorts",
        choices = c(""),
        selected = c(""),
        multiple = TRUE,
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
    shiny::conditionalPanel(
      condition = "input.tabs == 'temporalCharacterization' |
      input.tabs == 'conceptsInDataSource' |
      input.tabs == 'orphanConcepts'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("conceptSetsSelected"),
        label = "Concept sets",
        choices = c(""),
        selected = c(""),
        multiple = TRUE,
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
    )
  )

  return(panels)
}

#' The location of the description module helper file
#'
#' @details
#' Returns the location of the description helper file
#'
#' @return
#' string location of the description helper file
#' @family CohortDiagnostics
#'
#' @export
cohortDiagnosticsHelperFile <- function() {
  fileLoc <- system.file('cohort-diagnostics-www', "cohort-diagnostics.html", package = "CohortDiagnostics")
  return(fileLoc)
}


#' View for cohort diagnostics module
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#'
#' @return
#' The user interface to the cohort diagnostics viewer module
#' @family CohortDiagnostics
#'
#' @export
cohortDiagnosticsView <- function(id = "DiagnosticsExplorer") {
  ns <- shiny::NS(id)

  shiny::fluidPage(
    shinydashboard::box(
      title = "Cohort Level Diagnostics",
      width = "100%",
      shiny::fluidRow(
        shiny::column(
          shiny::selectInput(inputId = ns("tabs"),
                             label = "Select Report",
                             choices = c(), selected = NULL),
          width = 12
        ),
        shiny::column(
          cdUiControls(ns),
          width = 12
        )
      )
    ),
    shiny::tagList(
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'cohortDefinitions'",
        cohortDefinitionsView(ns("cohortDefinitions"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'cohortCounts'",
        cohortCountsView(ns("cohortCounts"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'indexEvents'",
        indexEventBreakdownView(ns("indexEvents"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'characterization'",
        cohortDiagCharacterizationView(ns("characterization"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'compareCohortCharacterization'",
        compareCohortCharacterizationView(ns("compareCohortCharacterization"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'cohortOverlap'",
        cohortOverlapView(ns("cohortOverlap"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'orphanConcepts'",
        orpahanConceptsView(ns("orphanConcepts"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'databaseInformation'",
        databaseInformationView(ns("databaseInformation"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'conceptsInDataSource'",
        conceptsInDataSourceView(ns("conceptsInDataSource"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'timeDistribution'",
        timeDistributionsView(id = ns("timeDistributions"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'visitContext'",
        visitContextView(ns("visitContext"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'incidenceRates'",
        incidenceRatesView(ns("incidenceRates"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.tabs == 'inclusionRules'",
        inclusionRulesView(ns("inclusionRules"))
      )
    )
  )
}
