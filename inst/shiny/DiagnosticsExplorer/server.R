shiny::shinyServer(function(input, output, session) {
  diagExpEnv$diagnosticsExplorerModule(id = "DiagnosticsExplorer",
                                       dataSource = diagExpEnv$dataSource,
                                       databaseTable = diagExpEnv$database,
                                       cohortTable = diagExpEnv$cohort,
                                       enableAnnotation = diagExpEnv$enableAnnotation,
                                       enableAuthorization = diagExpEnv$enableAuthorization,
                                       enabledTabs = diagExpEnv$enabledTabs,
                                       conceptSets = diagExpEnv$conceptSets,
                                       userCredentials = diagExpEnv$userCredentials,
                                       activeUser = diagExpEnv$activeUser)

})
