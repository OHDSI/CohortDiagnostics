shiny::shinyServer(function(input, output, session) {
  cdModule <- cohortDiagnosticsServer(id = "DiagnosticsExplorer",
                                      connectionHandler = connectionHandler,
                                      dataSource = dataSource,
                                      resultDatabaseSettings = shinySettings)


})
