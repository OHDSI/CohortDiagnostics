# library(magrittr)
# path = rstudioapi::getActiveProject()
# packageName <- "CohortDiagnostics"
# packageVersion <- "2.0"
# modelVersion <- "2.0"
# pathToCsvFiles <- tidyr::tibble(fullPath = list.files(path = path, pattern = ".csv", full.names = TRUE, recursive = TRUE)) %>% 
#   dplyr::mutate(baseName = basename(.data$fullPath) %>% stringr::str_remove_all(string = ., pattern = ".csv")) %>% 
#   dplyr::mutate(dontUse = dplyr::case_when(stringr::str_detect(string = baseName, pattern = "_", negate = TRUE) &
#                                              baseName != SqlRender::snakeCaseToCamelCase(baseName)
#                                            ~ TRUE,
#                                            TRUE ~ FALSE))
# 
# specification <- list()
# for (i in (1:nrow(pathToCsvFiles))) {
#   if (!pathToCsvFiles[i,]$dontUse) {
#     specification[[i]] <- CohortDiagnostics::guessCsvFileSpecification(pathToCsvFile = pathToCsvFiles[i,]$fullPath)
#   }
# }
# specification <- dplyr::bind_rows(specification)
# 
# resultsDataModelDirectory <- file.path(rstudioapi::getActiveProject(), 
#                                        "inst", 
#                                        "settings")
# readr::write_excel_csv(x = specification, 
#                        file = file.path(resultsDataModelDirectory, "resultsDataModelSpecification.csv"), 
#                        na = '')
# 
# #################
# # Please inspect the resultsDataModelSpecification.csv file and confirm if the guessed data model is accurate. 
# # Especially the keys.
# # The only keys that are valid are concept tables
# #################
# script <- CohortDiagnostics::createDdl(packageName = packageName, 
#                                        packageVersion = packageVersion,
#                                        modelVersion = modelVersion,
#                                        specification = readr::read_csv(file = file.path(resultsDataModelDirectory,                                                                                        "resultsDataModelSpecification.csv"),
#                                                                        col_types = readr::cols(), 
#                                                                        guess_max = min(1e7)) %>% 
#                                          dplyr::filter(!tableName %in% c('concept', 'conceptAncestor', 'conceptRelationship',
#                                                                          'concept_synonym', 'domain', 'relatioship', 'vocabulary'))
#                                        
#                                        )
# 
# pathToDdl <- file.path(rstudioapi::getActiveProject(), "inst", "sql", "sql_server")
# dir.create(pathToDdl, showWarnings = FALSE, recursive = TRUE)
# 
# SqlRender::writeSql(sql = script, 
#                     targetFile = file.path(pathToDdl, "sql_server_ddl_results_data_model.sql"))
# 
# 
# 
# 
# ################
# scriptConstraints <- CohortDiagnostics::createDdlPkConstraints(packageName = packageName, 
#                                                                packageVersion = packageVersion,
#                                                                modelVersion = modelVersion,
#                                                                specification = readr::read_csv(file = file.path(resultsDataModelDirectory,
#                                                                                                                 "resultsDataModelSpecification.csv"),
#                                                                                                guess_max = min(1e7),
#                                                                                                col_types = readr::cols()))
# 
# SqlRender::writeSql(sql = scriptConstraints, 
#                     targetFile = file.path(pathToDdl, "sql_server_ddl_results_data_model_constraints.sql"))
# 
# ################
# 
# scriptDropTable <- CohortDiagnostics::dropDdl(packageName = packageName, 
#                                               packageVersion = packageVersion,
#                                               modelVersion = modelVersion,
#                                               specification =  readr::read_csv(file = file.path(resultsDataModelDirectory,
#                                                                                                 "resultsDataModelSpecification.csv"),
#                                                                                guess_max = min(1e7),
#                                                                                col_types = readr::cols()))
# 
# SqlRender::writeSql(sql = scriptDropTable, 
#                     targetFile = file.path(pathToDdl, "sql_server_ddl_results_data_model_drop.sql"))
# 
