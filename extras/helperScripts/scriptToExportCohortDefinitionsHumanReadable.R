outputFolder <- "D:\\temp\\output"
atlasId <- c(1,2,3)

baseUrl <- Sys.getenv('baseUrl')
ROhdsiWebApi::authorizeWebApi(baseUrl = baseUrl, authMethod = 'windows')
cohortDefinitionsMetadata <- ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl) %>% 
  dplyr::filter(.data$id %in% atlasId)


for (i in (1:nrow(cohortDefinitionsMetadata))) {
  cohortId <- cohortDefinitionsMetadata[i,]$id
  cohortName <- cohortDefinitionsMetadata[i,]$name
  dir.create(path = file.path(outputFolder, cohortName), recursive = TRUE, showWarnings = FALSE)
  expression <- ROhdsiWebApi::getCohortDefinition(baseUrl = baseUrl, cohortId = cohortId)$expression
  details <-
    CohortDiagnostics::getCirceRenderedExpression(cohortDefinition = expression)
  SqlRender::writeSql(sql = details$cohortJson,
                      targetFile = file.path(outputFolder, cohortName, paste0('cohortDefinitionJson_', cohortId, '.json')))
  SqlRender::writeSql(sql = details$cohortMarkdown,
                      targetFile = file.path(outputFolder, cohortName, paste0('cohortDefinitionMarkdown_', cohortId, '.md')))
  SqlRender::writeSql(sql = details$conceptSetMarkdown,
                      targetFile = file.path(outputFolder, cohortName, paste0('conceptSetMarkdown_', cohortId, '.md')))
  SqlRender::writeSql(sql = details$cohortHtmlExpression,
                      targetFile = file.path(outputFolder, cohortName, paste0('cohortDefinitionHtml_', cohortId, '.html')))
  SqlRender::writeSql(sql = details$conceptSetHtmlExpression,
                      targetFile = file.path(outputFolder, cohortName, paste0('conceptSetHtml_', cohortId, '.html')))
}