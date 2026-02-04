#' The location of the home module helper file
#'
#' @details
#' Returns the location of the home helper file
#' @family Home
#' @return
#' string location of the home helper file
#'
#' @export
homeHelperFile <- function(){
  fileLoc <- system.file('home-www', "home.html", package = "CohortDiagnostics")
  return(fileLoc)
}

#' The module viewer for exploring summary results
#'
#' @details
#' The user specifies the id for the module
#' @family {Home}
#' @param id  the unique reference id for the module
#' 
#' @return
#' The user interface to the home viewer module
#'
#' @export
homeViewer <- function(id=1) {
  ns <- shiny::NS(id)
  
  shinydashboard::box(
    status = 'info', width = 12,
    title =  shiny::span( shiny::icon("house"), "Summary Reports"),
    solidHeader = TRUE,
    
    # tabs per html file
    shiny::uiOutput(ns("tabs"))
  )
  
}


#' The module server for exploring summary html files
#'
#' @details
#' The user specifies the id for the module
#'
#' @param id  the unique reference id for the module
#' @param connectionHandler a connection to the database with the results
#' @param resultDatabaseSettings a list containing the prediction result schema and connection details
#' @family Home
#' @return
#' The server for the home module
#'
#' @export
homeServer <- function(
    id, 
    connectionHandler,
    resultDatabaseSettings = list(port = 1)
) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
      # ShinyAppBuilder moves the html reports to the summaryReports folder
      # and renames them to the tabname.html
      # here we find all of these files as they end in html
      htmlFiles <- dir(Sys.getenv('shiny_report_folder'), pattern = '.html')
      
      # for each summary report in the summaryReports folder create a tab
      # containing the html
      
      if(length(htmlFiles) > 0){
      output$tabs <- shiny::renderUI({
        tabs <- list(NULL)
        for(i in 1:length(htmlFiles)){
          tabs[[i]] <- shiny::tabPanel(
            title = gsub('.html','', htmlFiles[i]),
            shiny::tags$iframe(
              src = file.path('www-reports',htmlFiles[i]), 
              style='width:90vw;height:100vh;'
            )
          )
             }
        do.call(shinydashboard::tabBox,tabs)
      })
      }
      

    }
  )
}
