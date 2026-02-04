# @file datasources-main.R
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




#' Define the helper file for the module
#'
#' @return The helper html file for the datasources module
#' @family Utils
#' 
#' @export
datasourcesHelperFile <- function() {
  fileLoc <-
    system.file('datasources-www', "datasources.html", package = "CohortDiagnostics")
  return(fileLoc)
}



#' The viewer function for hte datasources module
#'
#' @param id The unique id for the datasources viewer namespace
#'
#' @return The UI for the datasources module
#' @family Utils
#' 
#' @export
datasourcesViewer <- function(id) {
  ns <- shiny::NS(id)
  
  shinydashboard::box(
    status = 'info',
    width = "100%",
    title =  shiny::span(shiny::icon("database"), "Data Sources"),
    solidHeader = TRUE,

      shiny::tabsetPanel(
        type = 'pills',
        id = ns('mainPanel'),
        
        shiny::tabPanel(
          title = "Data Source Information",
           resultTableViewer(ns("datasourcesTable"))
        )
       )
      )
}




#' The server function for the datasources module
#'
#' @param id The unique id for the datasources server namespace
#' @param connectionHandler A connection to the database with the results
#' @param resultDatabaseSettings A named list containing the cohort generator results database details (schema, table prefix)
#'
#' @return The server for the datasources module
#' @family Utils
#' 
#' @export
datasourcesServer <- function(
  id, 
  connectionHandler, 
  resultDatabaseSettings
) {
  
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
      withTooltip <- function(value, tooltip, ...) {
        shiny::div(style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
                   tippy::tippy(value, tooltip, ...))
      }
      
      datasourcesData <- shiny::reactive({
        getDatasourcesData(
          connectionHandler = connectionHandler,
          resultDatabaseSettings = resultDatabaseSettings
      )
      })

      
      resultTableServer(
        id = "datasourcesTable",
        df = datasourcesData,
        colDefsInput = datasourcesColList(),
        selectedCols = c("databaseFullName", "databaseName", "cdmHolder",
                         "sourceReleaseDate", "cdmReleaseDate", "cdmVersion",
                         "vocabularyVersion", "maxObsPeriodEndDate"),
        downloadedFileName = "datasourcesTable-", 
        elementId = session$ns('datasourcesTable')
      )
      
      return(invisible(NULL))
      
      
      
      
    })
}

#pull database meta data table
getDatasourcesData <- function(
    connectionHandler, 
    resultDatabaseSettings
) {
  
  result <- OhdsiReportGenerator::getDatabaseDetails(
    connectionHandler = connectionHandler,
    schema = resultDatabaseSettings$schema,
    databaseTable = resultDatabaseSettings$databaseTable
  )
  
  return(result)
}


datasourcesColList <- function(){
  
  result <- list(
    databaseFullName = reactable::colDef(
      name = "Full DB Name", 
      header = withTooltip(
        "Full DB Name",
        "Name of the database (DB)"
      )
      ),
    databaseName = reactable::colDef(
      name = "DB Name", 
      header = withTooltip(
        "DB Name",
        "Abbreviation for the database (DB)"
      )
    ),
    cdmHolder = reactable::colDef(
      name = "DB Holder", 
      header = withTooltip(
        "DB Holder",
        "Holder of the database (DB)"
      )
    ),
    sourceDescription = reactable::colDef(
      name = "DB Description", 
      minWidth = 500,
      header = withTooltip(
        "DB Description",
        "Description of the database (DB)"
      )
    ),
    sourceDocumentationReference = reactable::colDef(
      name = "DB Description Link", 
      header = withTooltip(
        "DB Description Link",
        "HTML link to the database (DB) description"
      ),
      html = TRUE, 
      cell = function(value, index){
        if(value != 'None'){
          shiny::tagList(
            shiny::a("RHEALTH Description", href = value, target = "_blank")
          )
        } else{
          'No link available'
        }
      }
    ),

    cdmEtlReference = reactable::colDef(
      name = "DB ETL Link", 
      header = withTooltip(
        "DB ETL Link",
        "HTML link to the ETL for the database (DB)"
      ),
      html = TRUE, 
      cell = function(value, index){
        if(value != 'None'){
          shiny::tagList(
            shiny::a("RHEALTH Description", href = value, target = "_blank")
          )
        } else{
          'No link available'
        }
      }
    ),
    sourceReleaseDate = reactable::colDef(
      name = "Source Data Release Date", 
      header = withTooltip(
        "Source Data Release Date",
        "Date the source data was released"
      )
    ),
    cdmReleaseDate = reactable::colDef(
      name = "CDM DB Release Date", 
      header = withTooltip(
        "CDM DB Release Date",
        "Date the CDM database (DB) was accessible"
      )
    ),
    cdmVersion = reactable::colDef(
      name = "CDM Version", 
      header = withTooltip(
        "CDM Version",
        "Version of the common data model (CDM)"
      )
    ),
    cdmVersionConceptId = reactable::colDef(
      show = FALSE
    ),
    vocabularyVersion = reactable::colDef(
      name = "Vocabulary Version", 
      header = withTooltip(
        "Vocabulary Version",
        "Version of the vocabulary used in the database (DB)"
      )
    ),
    maxObsPeriodEndDate = reactable::colDef(
      name = "Max Obs. Period End Date", 
      header = withTooltip(
        "Max Obs. Period End Date",
        "Maximum/Latest observation period date in the database (DB)"
      )
    ),
    databaseId = reactable::colDef(
        name = "DB ID", 
        header = withTooltip(
          "DB ID",
          "Unique identifier (ID) of the database (DB)"
        )
      )
  )
  
  
  return(result)
}

