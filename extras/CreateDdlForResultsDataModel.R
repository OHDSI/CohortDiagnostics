library(magrittr)
pathToCsvFiles <- tidyr::tibble(fullPath = list.files(path = path, pattern = ".csv", full.names = TRUE)) %>% 
  dplyr::mutate(baseName = basename(.data$fullPath) %>% stringr::str_remove_all(string = ., pattern = ".csv")) %>% 
  dplyr::mutate(dontUse = dplyr::case_when(stringr::str_detect(string = baseName, pattern = "_", negate = TRUE) &
                                             baseName != SqlRender::snakeCaseToCamelCase(baseName)
                                           ~ TRUE,
                                           TRUE ~ FALSE))

specification <- list()
for (i in (1:nrow(pathToCsvFiles))) {
  if (!pathToCsvFiles[i,]$dontUse) {
    specification[[i]] <- CohortDiagnostics::guessCsvFileSpecification(pathToCsvFile = pathToCsvFiles[i,]$fullPath)
  }
}
specification <- dplyr::bind_rows(specification)

readr::write_csv(x = specification, 
                 path = "inst/sql/resultsDataModel/specification.csv")

#################
# Please inspect the specification csv file and confirm if the guessed data model is accurate. Especially the keys
# The only keys that are valid are concept tables
#################
script <- CohortDiagnostics::createDdl(packageName = 'CohortDiagnostics', 
                                       packageVersion = '2.0',
                                       modelVersion = '2.0',
                                       specification = readr::read_csv(file = "inst/sql/resultsDataModel/specification.csv",
                                                                       col_types = readr::cols()))

pathToDdl <- "inst/sql/resultsDataModel/"
dir.create(pathToDdl, showWarnings = FALSE, recursive = TRUE)

SqlRender::writeSql(sql = script, 
                    targetFile = file.path(pathToDdl, "sql_server_ddl.sql"))


SqlRender::translateSqlFile(sourceFile = file.path(pathToDdl, "sql_server_ddl.sql"), 
                            targetFile = file.path(pathToDdl, "postgresql_ddl.sql"),
                            targetDialect = "postgresql")

################
scriptConstraints <- CohortDiagnostics::createDdlPkConstraints(packageName = 'CohortDiagnostics', 
                                                               packageVersion = '2.0',
                                                               modelVersion = '2.0',
                                                               specification = readr::read_csv(file = "inst/sql/resultsDataModel/specification.csv",
                                                                                               col_types = readr::cols()))

SqlRender::writeSql(sql = scriptConstraints, 
                    targetFile = file.path(pathToDdl, "sql_server_ddl_constraints.sql"))


SqlRender::translateSqlFile(sourceFile = file.path(pathToDdl, "sql_server_ddl_constraints.sql"), 
                            targetFile = file.path(pathToDdl, "postgresql_ddl_constraints.sql"),
                            targetDialect = "postgresql")

################

scriptDropTable <- CohortDiagnostics::dropDdl(packageName = 'CohortDiagnostics', 
                                                               packageVersion = '2.0',
                                                               modelVersion = '2.0',
                                                               specification = readr::read_csv(file = "inst/sql/resultsDataModel/specification.csv",
                                                                                               col_types = readr::cols()))

SqlRender::writeSql(sql = scriptDropTable, 
                    targetFile = file.path(pathToDdl, "sql_server_ddl_drop.sql"))

SqlRender::translateSqlFile(sourceFile = file.path(pathToDdl, "sql_server_ddl_drop.sql"), 
                            targetFile = file.path(pathToDdl, "postgresql_ddl_drop.sql"),
                            targetDialect = "postgresql")

