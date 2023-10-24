shiny::shinyServer(function(input, output, session) {
  cdModule <- OhdsiShinyModules::cohortDiagnosticsServer(id = "DiagnosticsExplorer",
                                                        connectionHandler = connectionHandler,
                                                        dataSource = dataSource,
                                                        resultDatabaseSettings = shinySettings)


})
