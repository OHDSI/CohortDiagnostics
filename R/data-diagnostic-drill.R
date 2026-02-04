# @file data-diagnostic-drill.R
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


#' The module viewer for exploring data-diagnostic results in more detail
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
dataDiagnosticDrillViewer <- function(id) {
  ns <- shiny::NS(id)
  
  #shiny::div(
  shiny::fluidPage(
    shiny::column(
      width = 2,
      shiny::uiOutput(ns('dataDiagnosticInputs'))
    ),
    shiny::column(
      width = 10,
      reactable::reactableOutput(ns('drugStudyFailSummaryTable'), width = '100%')
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
dataDiagnosticDrillServer <- function(
    id, 
    connectionHandler,
    resultDatabaseSettings
) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
    
      analyses <- getAnalysisNames(
        connectionHandler = connectionHandler, 
        resultDatabaseSettings = resultDatabaseSettings
      )
      
      databases <- shiny::reactiveVal({
        getDbDataDiagnosticsDatabases(
          connectionHandler = connectionHandler, 
          resultDatabaseSettings = resultDatabaseSettings,
          analysisName = analyses[1]
        )
      })
      
      analysis <- shiny::reactiveVal({
        analyses[1]
      })
      
      output$dataDiagnosticInputs <- shiny::renderUI({  
        
        shiny::tagList(
          shiny::selectInput(
            inputId = session$ns("analysisSelected"), 
            label = shiny::h4("Analysis:"), 
            choices =  analyses, 
            selected = analysis()
          ),
          
          shiny::checkboxGroupInput(
            inputId = session$ns("databasesSelected"), 
            label = shiny::h4("Databases:"), 
            choiceNames =  databases(),
            choiceValues  =  databases(),
            selected = databases(),
            inline = FALSE,
            width = NULL
          )
        )
        
      })

      # inital table
      resultTable <- shiny::reactiveVal(
        value = getDrugStudyFail(
          connectionHandler = connectionHandler, 
          resultDatabaseSettings = resultDatabaseSettings,
        analysis = analyses[1]
      ))
      
      shiny::observeEvent(input$analysisSelected, {
        
        if(!is.null(input$analysisSelected)){
          
          resultTableTemp <- getDrugStudyFail(
            connectionHandler = connectionHandler, 
            resultDatabaseSettings = resultDatabaseSettings,
            analysis = input$analysisSelected
          )
          resultTable(resultTableTemp)
          
          databases(unique(resultTableTemp$databaseId))
          
          analysis(input$analysisSelected)
          
        }
      }) # end observe event analysis
      
    
      output$drugStudyFailSummaryTable <- reactable::renderReactable({
        
        if(!is.null(resultTable())){
          
          
          cinds <- ! (colnames(resultTable()) %in% c('databaseId', 'minSampleSize', 'maxSampleSize',"analysisId", "analysisName"))
          
          columnFormat2 <- lapply(
            X = 1:ncol(resultTable()[, cinds]), 
            
            FUN = function(x){
              return(
                reactable::colDef(
                  style = function(value) {
                    if(is.na(value)){
                      color <- '#84848c'
                    } else {
                      if (value > 0) {
                        color <- '#e00000'
                      } else {
                        color <- "#008000"
                      }
                    }
                    list(color = color, fontWeight = "bold")
                  }
                )
              )
            }
            
          )
          names(columnFormat2) <- colnames(resultTable()[, cinds])
           
            reactable::reactable(
              data = resultTable() %>% 
                dplyr::select(-c("analysisId", "analysisName")) %>%
                dplyr::filter(.data$databaseId %in% input$databasesSelected) %>%
                dplyr::mutate(view  = '') %>%
                dplyr::relocate("view", .before = 'databaseId'),
              defaultPageSize = 20,
              searchable = TRUE,
              columns = c(
                list(
                  view = reactable::colDef(
                  name = "",
                  sortable = FALSE,
                  cell = function() shiny::tags$button("View")
                )), 
                columnFormat2
              ),
              onClick = reactable::JS(paste0("function(rowInfo, column) {
    // Only handle click events on the 'details' column
    if (column.id !== 'view') {
      return
    }
    // Send the click event to Shiny, which will be available in input$show_details
    // Note that the row index starts at 0 in JavaScript, so we add 1
      Shiny.setInputValue('",session$ns('show_details'),"', { index: rowInfo.index + 1 }, { priority: 'event' })
  }")
              )
            )

        }
        
      }) # end reactable
      
      
      #  Now add module to show details: input$show_details
      shiny::observeEvent(input$show_details, {
        
        database <- resultTable() %>% 
          dplyr::select("databaseId") %>%
          dplyr::filter(.data$databaseId %in% input$databasesSelected)
        
        databaseId <- database$databaseId[input$show_details$index]
        analysisName <- input$analysisSelected
        
        output$modaltable <- reactable::renderReactable({
          reactable::reactable(
            data = getDrillDown(
              connectionHandler = connectionHandler, 
              resultDatabaseSettings = resultDatabaseSettings,
              analysisName = analysisName, 
              databaseId = databaseId
            )
          )
        }
        )

        shiny::showModal(
          shiny::modalDialog(
            title = "Details",
            paste0("For database: ", databaseId, " and analysisName ", analysisName),

            reactable::reactableOutput(session$ns("modaltable")),
        
            easyClose = TRUE,
            footer = NULL,
            size = "l"
            
          )
        )
           
        
      })
      
      
    }
  )
}

getDrillDown <- function(
    connectionHandler, 
    resultDatabaseSettings,
  analysisName, 
  databaseId
){
  
  sql <- "SELECT * FROM @schema.@dd_table_prefixdata_diagnostics_output
  WHERE analysis_name = '@analysis_name' and database_id = '@database_id';"
  
  result <- connectionHandler$queryDb(
    sql = sql, 
    schema = resultDatabaseSettings$schema,
    dd_table_prefix = resultDatabaseSettings$ddTablePrefix,
    analysis_name = analysisName,
    database_id = databaseId
  )
  
  result <- result %>%
    dplyr::select(-c("databaseId", "analysisId", "analysisName"))
  
  return(result)
}

getDbDataDiagnostics <- function(
    connectionHandler, 
    resultDatabaseSettings 
){
  
  sql <- "SELECT distinct database_id FROM @schema.@dd_table_prefixdata_diagnostics_summary;"

  dbNames <- connectionHandler$queryDb(
    sql = sql, 
    schema = resultDatabaseSettings$schema,
    dd_table_prefix = resultDatabaseSettings$ddTablePrefix
  )
  result <- list(dbNames$databaseId)
  
  return(result)
  
}

getDrugStudyFail <- function(
    connectionHandler, 
    resultDatabaseSettings,
    analysis = NULL
){
  
  if(is.null(analysis)){
    return(NULL)
  }
  
  shiny::withProgress(message = 'Extracting data diagnostic summary', value = 0, {
    
    shiny::incProgress(1/3, detail = paste("Extracting data"))
    
    sql <- "SELECT * FROM @schema.@dd_table_prefixdata_diagnostics_summary
  WHERE analysis_name in ('@analysis');"
    
    summaryTable <- connectionHandler$queryDb(
      sql = sql, 
      schema = resultDatabaseSettings$schema,
      dd_table_prefix = resultDatabaseSettings$ddTablePrefix,
      analysis = analysis
    )
    
    shiny::incProgress(2/3, detail = paste("Data extracted"))
    
    # hide analysisId and analysisName
    
    shiny::incProgress(3/3, detail = paste("Finished"))
    
    ParallelLogger::logInfo("Got database diagnostic fail summary")
    
  })
  
  return(summaryTable)  
}


getDbDataDiagnosticsDatabases <- function(
  connectionHandler, 
  resultDatabaseSettings,
  analysisName
){
  if(!is.null(analysisName)){
    sql <- "SELECT distinct database_id FROM @schema.@dd_table_prefixdata_diagnostics_summary
  WHERE analysis_name in ('@analysis');"
    
    res <- connectionHandler$queryDb(
      sql = sql, 
      schema = resultDatabaseSettings$schema,
      dd_table_prefix = resultDatabaseSettings$ddTablePrefix,
      analysis = analysisName
    )
    
    return(res$databaseId)
  } else{
    return(c())
  }
}
