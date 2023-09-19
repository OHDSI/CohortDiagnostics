shiny::shinyServer(function(input, output, session) {
  cdModule <- OhdsiShinyModules::cohortDiagnosticsSever(id = "DiagnosticsExplorer",
                                                        connectionHandler = connectionHandler,
                                                        dataSource = dataSource,
                                                        resultDatabaseSettings = shinySettings)


})
