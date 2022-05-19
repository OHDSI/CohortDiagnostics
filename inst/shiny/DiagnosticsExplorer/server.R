shiny::shinyServer(function(input, output, session) {
  diagnosticsExplorerModule(id = "DiagnosticsExplorer",
                            dataSource = dataSource,
                            databaseTable = database,
                            cohortTable = cohort,
                            enableAnnotation = enableAnnotation,
                            enableAuthorization = enableAuthorization,
                            enabledTabs = enabledTabs,
                            conceptSets = conceptSets,
                            userCredentials = userCredentials,
                            activeUser = activeUser)

})
