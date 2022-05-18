shiny::shinyServer(function(input, output, session) {
  diagnosticsExplorerModule("DiagnosticsExplorer",
                            dataSource,
                            database,
                            cohort,
                            enableAnnotation,
                            enableAuthorization)

})
