shiny::shinyServer(function(input, output, session) {
  diagExpEnv$diagnosticsExplorerModule(id = "DiagnosticsExplorer",
                                       envir = diagExpEnv)
})
