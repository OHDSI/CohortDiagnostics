# @file data-diagnostic-summary.R
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


#' The module viewer for exploring data-diagnostic summary results 
#'
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#' @family DataDiagnostics
#' @return
#' The user interface to the summary module
#'
#' @export
dataDiagnosticSummaryViewer <- function(id) {
  ns <- shiny::NS(id)
  
  shiny::div(
    shiny::fluidPage(
      shiny::uiOutput(ns('dbDiagInputs')),
      reactable::reactableOutput(ns('drugStudyFailSummaryTable'))
    )
  )
}

#' The module server for exploring prediction summary results 
#'
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#' @param connectionHandler the connection to the prediction result database
#' @param resultDatabaseSettings a list containing the result schema and prefixes
#' @family DataDiagnostics
#' @return
#' The server to the summary module
#'
#' @export
dataDiagnosticSummaryServer <- function(
    id, 
    connectionHandler,
    resultDatabaseSettings
) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
      analysisNames <- getAnalysisNames(
        connectionHandler = connectionHandler, 
        resultDatabaseSettings = resultDatabaseSettings
      )
      
      # create UI for selecting analysis of interest
      output$dbDiagInputs <- shiny::renderUI({
          shiny::fluidRow(
            shiny::selectInput(
              inputId = session$ns('dbDiagAnalysisNameSelected'), 
              label = 'Analysis:', 
              choices = analysisNames, 
              multiple = T, 
              selected = analysisNames[1]
            )
        )
      }
      )
      
      resultTable <- shiny::reactive(
        if(!is.null(input$dbDiagAnalysisNameSelected[1])){
        getDrugStudyFailSummary(
          connectionHandler = connectionHandler, 
          resultDatabaseSettings = resultDatabaseSettings,
          analysisNames = input$dbDiagAnalysisNameSelected
        )
        } else{
          data.frame(databaseId = 'none', analysis = 'none')
        }
      )
      
      # create the colum formating
      columnFormat <- shiny::reactive({
        
        if(!is.null(input$dbDiagAnalysisNameSelected[1])){
          res <- lapply(
            X = 1:ncol(resultTable()[,-1]), 
            
            FUN = function(x){
              return(
                reactable::colDef(
                  style = function(value) {
                    if (value > 0) {
                      color <- '#e00000'
                    } else {
                      color <- "#008000"
                    }
                    list(color = color, fontWeight = "bold")
                  }
                )
              )
            }
            
          )
          names(res) <- colnames(resultTable()[,-1])
          return(res)
        } else{
          return(NULL)
        }
      })
      
      # format this to be color based on number
      output$drugStudyFailSummaryTable <- reactable::renderReactable({
        reactable::reactable(
          data = resultTable(),
          defaultPageSize = 20,
          searchable = TRUE,
          columns = columnFormat()
        )
 
      })
      
    }
  )
}

getAnalysisNames <- function(
    connectionHandler, 
    resultDatabaseSettings
    ){
  
  sql <- "SELECT distinct sum.analysis_name 
    FROM @schema.@dd_table_prefixdata_diagnostics_summary as sum;"
  
  result <- connectionHandler$queryDb(
    sql = sql, 
    schema = resultDatabaseSettings$schema,
    dd_table_prefix = resultDatabaseSettings$ddTablePrefix
  )

  return(sort(result$analysisName))
}

getDrugStudyFailSummary <- function(
    connectionHandler, 
    resultDatabaseSettings,
    analysisNames
){
  

  
  shiny::withProgress(message = 'Extracting data diagnostic summary', value = 0, {
    
    shiny::incProgress(1/3, detail = paste("Extracting data"))
    
    sql <- "SELECT distinct 
     sum.analysis_id, 
     sum.analysis_name, 
     sum.database_id,
     sum.total_fails
    
    FROM @schema.@dd_table_prefixdata_diagnostics_summary as sum
    where sum.analysis_name in (@names);"
    
    summaryTable <- connectionHandler$queryDb(
      sql = sql, 
      schema = resultDatabaseSettings$schema,
      dd_table_prefix = resultDatabaseSettings$ddTablePrefix,
      names = paste(paste0("'",analysisNames,"'"), collapse=',')
    )
    
    shiny::incProgress(2/3, detail = paste("Data extracted"))
  
    
    summaryTable <- tidyr::pivot_wider(
      data = summaryTable, 
      id_cols = 'databaseId', 
      names_from = 'analysisName', 
      values_from = 'totalFails', 
      values_fill = -1
      )
    
    shiny::incProgress(3/3, detail = paste("Finished"))
    
    ParallelLogger::logInfo("Got database diagnostic summary")
    
  })
  
  return(summaryTable)  
}
