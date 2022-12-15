shiny::shinyServer(function(input, output, session) {
  cdModule <- OhdsiShinyModules::cohortDiagnosticsSever(id = "DiagnosticsExplorer",
                                                        dataSource = dataSource,
                                                        resultsDatabaseSettings = shinySettings)


})
