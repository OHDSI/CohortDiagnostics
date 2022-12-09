shiny::shinyServer(function(input, output, session) {

  OhdsiShinyModules::cohortDiagnosticsSever(id = "DiagnosticsExplorer",
                                            connectionHandler,
                                            shinySettings)


})
