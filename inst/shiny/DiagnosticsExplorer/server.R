shiny::shinyServer(function(input, output, session) {
  cdModule <- CohortDiagnostics::cohortDiagnosticsServer(
    id = "DiagnosticsExplorer",
    connectionHandler = connectionHandler,
    dataSource = dataSource,
    resultDatabaseSettings = shinySettings
  )
})
