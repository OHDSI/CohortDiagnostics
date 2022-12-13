shiny::shinyServer(function(input, output, session) {
  OhdsiShinyModules::cohortDiagnosticsSever(id = "DiagnosticsExplorer",
                                            dataSource = dataSource,
                                            resultsDatabaseSettings = shinySettings)
})
