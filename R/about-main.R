# @file about-main.R
#
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


#' The location of the about module helper file
#'
#' @details
#' Returns the location of the about helper file
#' @family About
#' @return
#' string location of the about helper file
#'
#' @export
aboutHelperFile <- function() {
  fileLoc <-
    system.file('about-www', "about.html", package = "CohortDiagnostics")
  return(fileLoc)
}

#' The module viewer for the shiny app home
#'
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#' @family About
#' @return
#' The user interface to the home page module
#'
#' @export
aboutViewer <- function(id = 'homepage') {
  ns <- shiny::NS(id)
  
  #shinydashboard::dashboardBody(
  shiny::div(
    shiny::fluidRow(
      shiny::tags$head(shiny::tags$style(
        shiny::HTML(".small-box {height: 200px; width: 100%;}")
      )),
      shinydashboard::box(width = "100%",
                          shiny::htmlTemplate(
                            system.file("about-www", "about.html", package = utils::packageName())
                          ))
      # )
    ),
    shiny::fluidRow(
      shinydashboard::valueBoxOutput(ns("datasourcesBox"), width = 3),
      shinydashboard::valueBoxOutput(ns("cohortsBox"), width = 3),
      shinydashboard::valueBoxOutput(ns("characterizationBox"), width = 3),
      shinydashboard::valueBoxOutput(ns("cohortDiagnosticsBox"), width = 3)
    ),
    shiny::fluidRow(
      shinydashboard::valueBoxOutput(ns("estimationBox"), width = 3),
      shinydashboard::valueBoxOutput(ns("predictionBox"), width = 3),
      shinydashboard::valueBoxOutput(ns("reportGeneratorBox"), width = 3)
      # ,
      # shinydashboard::valueBoxOutput(ns("sccsBox"), width = 3),
      # shinydashboard::valueBoxOutput(ns("evidenceSynthesisBox"), width = 3)
    )
  )
}

targetedValueBox <- function(
    value,
    subtitle,
    icon,
    color,
    href,
    target = "_new"
  ) {
  valueBox <- shinydashboard::valueBox(
    value = value,
    subtitle = subtitle,
    icon = icon,
    color = color,
    href = href
  )
  shiny::tagAppendAttributes(valueBox,.cssSelector="a", target=target)
}

#' The module server for the shiny app home
#'
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#' @param connectionHandler a connection to the database with the results
#' @param resultDatabaseSettings a list containing the characterization result schema, dbms, tablePrefix, databaseTable and cgTablePrefix
#' @param config the config from the app.R file that contains a list of which modules to include
#' @family About
#' @return
#' The server for the shiny app home
#'
#' @export
aboutServer <- function(id = 'homepage',
                        connectionHandler = NULL,
                        resultDatabaseSettings = NULL,
                        config) {
  shiny::moduleServer(id,
                      function(input, output, session) {
                        tab_names <- character()
                        # Loop through shinyModules and extract tabName values
                        for (i in seq_along(config[["shinyModules"]])) {
                          tab_name <- config[["shinyModules"]][[i]][["tabName"]]
                          tab_names <- c(tab_names, tab_name)
                        }
                        # View the extracted tabName values
                        # print(tab_names)
                        
                        output$datasourcesBox <-
                          shinydashboard::renderValueBox({
                            if ("DataSources" %in% tab_names) {
                              targetedValueBox(
                                value = "Data Sources",
                                subtitle = "Data sources used in this analysis",
                                icon = shiny::icon("database"),
                                color = "olive",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/DataSources.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Data Sources",
                                subtitle = "This module was not included in this analysis",
                                icon = shiny::icon("database"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/DataSources.html"
                              )
                            }
                          })
                        
                        output$cohortsBox <-
                          shinydashboard::renderValueBox({
                            if ("Cohorts" %in% tab_names) {
                              targetedValueBox(
                                value = "Cohorts",
                                subtitle = "Cohorts included in this analysis",
                                icon = shiny::icon("user-gear"),
                                color = "purple",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Cohorts.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Cohorts",
                                subtitle = "This module was not included in this analysis",
                                icon = shiny::icon("user-gear"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Cohorts.html"
                              )
                            }
                          })
                        
                        output$characterizationBox <-
                          shinydashboard::renderValueBox({
                            if ("Characterization" %in% tab_names) {
                              targetedValueBox(
                                value = "Characterization",
                                subtitle = "Characterization results for this analysis",
                                icon = shiny::icon("table"),
                                color = "red",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Characterization.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Characterization",
                                subtitle = "This module was not included in this analysis",
                                icon = shiny::icon("table"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Characterization.html"
                              )
                            }
                          })
                        
                        output$cohortDiagnosticsBox <-
                          shinydashboard::renderValueBox({
                            if ("CohortDiagnostics" %in% tab_names) {
                              targetedValueBox(
                                value = "Cohort Diagnostics",
                                subtitle = "Cohort Diagnostics results for the cohorts included in this analysis",
                                icon = shiny::icon("users"),
                                color = "yellow",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/CohortDiagnostics.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Cohort Diagnostics",
                                subtitle = "This module was not included in this analysis",
                                icon = shiny::icon("users"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/CohortDiagnostics.html"
                              )
                            }
                          })
                        
                        
                        output$estimationBox <-
                          shinydashboard::renderValueBox({
                            if ("Estimation" %in% tab_names) {
                              targetedValueBox(
                                value = "Estimation",
                                subtitle = "Population-level effect estimation results for this analysis",
                                icon = shiny::icon("chart-column"),
                                color = "maroon",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Estimation.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Estimation",
                                subtitle = "This module was not included in this analysis",
                                icon = shiny::icon("chart-column"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Estimation.html"
                              )
                            }
                          })
                        
                        output$predictionBox <-
                          shinydashboard::renderValueBox({
                            if ("Prediction" %in% tab_names) {
                              targetedValueBox(
                                value = "Prediction",
                                subtitle = "Patient-level prediction results for this analysis",
                                icon = shiny::icon("chart-line"),
                                color = "blue",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Prediction.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Prediction",
                                subtitle = "This module was not included in this analysis",
                                icon = shiny::icon("chart-line"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/Prediction.html"
                              )
                            }
                          })
                        
                        output$sccsBox <-
                          shinydashboard::renderValueBox({
                            if ("SCCS" %in% tab_names) {
                              targetedValueBox(
                                value = "SCCS",
                                subtitle = "Self-Controlled Case Series results for this analysis",
                                icon = shiny::icon("people-arrows"),
                                color = "red",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/SelfControlledCaseSeries.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "SCCS",
                                subtitle = "This module was not included in this analysis",
                                icon = shiny::icon("people-arrows"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/SelfControlledCaseSeries.html"
                              )
                            }
                          })
                        
                        output$evidenceSynthesisBox <-
                          shinydashboard::renderValueBox({
                            if ("Meta" %in% tab_names) {
                              targetedValueBox(
                                value = "Meta",
                                subtitle = "Meta Analysis results for this analysis",
                                icon = shiny::icon("sliders"),
                                color = "olive",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/EvidenceSynthesis.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Meta",
                                subtitle =
                                  "This module was not included in this analysis",
                                icon = shiny::icon("sliders"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/EvidenceSynthesis.html"
                              )
                            }
                          })
                        
                        output$reportGeneratorBox <-
                          shinydashboard::renderValueBox({
                            if ("Report" %in% tab_names) {
                              targetedValueBox(
                                value = "Report",
                                subtitle = "Report Generator for this analysis",
                                icon = shiny::icon("book"),
                                color = "teal",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/ReportGenerator.html"
                              )
                            } else {
                              targetedValueBox(
                                value = "Report",
                                subtitle =
                                  "This module was not included in this analysis",
                                icon = shiny::icon("book"),
                                color = "black",
                                href = "https://ohdsi.github.io/OhdsiShinyModules/articles/ReportGenerator.html"
                              )
                            }
                          })
                        
                      })
}
