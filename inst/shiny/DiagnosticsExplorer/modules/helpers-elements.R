##
# File for general elements to be used throughout application to support consistent visual design choices
##

withTooltip <- function(value, tooltip, ...) {
  shiny::div(style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
             tippy::tippy(value, tooltip, ...))
}

reactableCsvDownloadButton <- function(ns,
                                       outputTableId,
                                       buttonText = "Download as CSV") {

  shiny::tagList(
    shiny::div(
      style = "text-align:right;",
      withTooltip(shiny::tags$button(buttonText,
                                     onclick = paste0("Reactable.downloadDataCSV('", ns(outputTableId), "')")),
                  tooltip = "Note, will not download live values filtered in table, groupings, or any graphical/stylstic elements")
    )
  )
}
