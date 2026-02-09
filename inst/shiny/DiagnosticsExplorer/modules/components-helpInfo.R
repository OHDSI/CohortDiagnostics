infoHelperViewer <- function(
    id = "helper",
    helpLocation = system.file("cohort-diagnostics-www", "cohort-diagnostics.html", package = "CohortDiagnostics")
    ) {
  ns <- shiny::NS(id)
  
shinydashboard::box(
  collapsible = TRUE,
  collapsed = TRUE,
  title = shiny::span( shiny::icon("circle-question"), "Help & Information"),
  width = "100%",
  shiny::htmlTemplate(helpLocation)
)
}
