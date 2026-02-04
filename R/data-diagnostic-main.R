# @file data-diagnostic-main.R
#
# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of OhdsiShinyModules
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


#' The location of the data-diagnostic module helper file
#'
#' @details
#' Returns the location of the data-diagnostic helper file
#' @family DataDiagnostics
#' @return
#' string location of the data-diagnostic helper file
#'
#' @export
dataDiagnosticHelperFile <- function(){
  fileLoc <- system.file('data-diagnostic-www', "data-diagnostic.html", package = "CohortDiagnostics")
  return(fileLoc)
}

#' The module viewer for exploring data-diagnostic
#'
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#' @family DataDiagnostics
#' @return
#' The user interface to the data-diagnostic viewer module
#'
#' @export
dataDiagnosticViewer <- function(id = 'dataDiag') {
  ns <- shiny::NS(id)

  shiny::tabsetPanel(
    header = shiny::h1("Data Diagnostic Explorer"),
    id = ns('main_tabs'),
    type = "pills",
    
    shiny::tabPanel(
      "Summary",  
      dataDiagnosticSummaryViewer(ns('summary-tab'))
    ),
    
    shiny::tabPanel(
      "Drill-Down",  
      dataDiagnosticDrillViewer(ns('drill-down-tab'))
    )
    
  )
  
}

#' The module server for exploring data-diagnostic
#'
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#' @param connectionHandler a connection to the database with the results
#' @param resultDatabaseSettings a list containing the data-diagnostic result schema
#' @family DataDiagnostics
#' @return
#' The server for the data-diagnostic module
#'
#' @export
dataDiagnosticServer <- function(
  id = 'dataDiag', 
  connectionHandler,
  resultDatabaseSettings = list(port = 1)
) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
      #==========================
      #    SERVERS
      #==========================
      
      dataDiagnosticSummaryServer(
        id = 'summary-tab', 
        connectionHandler = connectionHandler, 
        resultDatabaseSettings = resultDatabaseSettings
        )
      
      dataDiagnosticDrillServer(
        id = 'drill-down-tab', 
        connectionHandler = connectionHandler, 
        resultDatabaseSettings = resultDatabaseSettings
      )
    
      
    }
  )
}
