jsonFolder <- "D:\\git\\github\\ohdsi\\CohortDiagnostics\\inst\\cohorts"
listJsonFiles <- list.files(path = jsonFolder, pattern = ".json", full.names = TRUE, recursive = FALSE)

options <- CirceR::createGenerateOptions(
  generateStats = TRUE
)

for (i in (1:length(listJsonFiles))) {
  jsonFile <- SqlRender::readSql(listJsonFiles[[i]])
  expression <- CirceR::cohortExpressionFromJson(expressionJson = jsonFile)
  sql <- CirceR::buildCohortQuery(expression = expression, options = options)
  sqlFileName <- stringr::str_replace(string = basename(listJsonFiles[[i]]),
                       pattern = '.json',
                       replacement = '.sql')
  path <- "D:\\git\\github\\ohdsi\\CohortDiagnostics\\inst\\sql\\sql_server"
  
  SqlRender::writeSql(sql = sql,
                      targetFile = file.path(path, sqlFileName))
  
}
