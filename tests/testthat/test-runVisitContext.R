library(SqlRender)
library(readxl)

# table_exists <- function(connection, table_name){
#   sql <- "SELECT * FROM @table_name"
# 
#   result <- DatabaseConnector::renderTranslateQuerySql(
#       connection = connection,
#       sql = sql,
#       table_name = table_name,
#       snakeCaseToCamelCase = TRUE
#   )
# }
# 
# 
# for (nm in names(testServers)) {
#   
#   server <- testServers[[nm]]
#   
#   con <- DatabaseConnector::connect(server$connectionDetails)
#   
#   exportFolder <- tempfile()
#   
#   dir.create(exportFolder)
#   
#   test_that(paste("test temporary table #concept_ids creation"), {
# 
#     getVisitContext(connection = con,
#                     cdmDatabaseSchema = server$cdmDatabaseSchema,
#                     tempEmulationSchema = server$tempEmulationSchema,
#                     cohortDatabaseSchema = server$cohortDatabaseSchema,
#                     cohortTable =  server$cohortTable,
#                     cohortIds = server$cohortIds,
#                     conceptIdTable = "#concept_ids",
#                     cdmVersion = 5
#                     )
# 
#   expect_no_error(table_exists(con, "#concept_ids"))
# 
#   })
# 
# 
#   test_that(paste("test no duplicates in concept_ids table for getVisitContext function"), {
# 
#     sql <-  "SELECT * FROM #concept_ids"
# 
# 
#     translatedSql <- translate(sql, targetDialect = server$connectionDetails$dbms)
# 
# 
#     firstTime <- system.time(
# 
# 
#     visitContextResult <- getVisitContext(connection = con,
#                                           cdmDatabaseSchema = server$cdmDatabaseSchema,
#                                           tempEmulationSchema = server$tempEmulationSchema,
#                                           cohortDatabaseSchema = server$cohortDatabaseSchema,
#                                           cohortTable =  server$cohortTable,
#                                           cohortIds = server$cohortIds,
#                                           conceptIdTable = "#concept_ids",
#                                           cdmVersion = 5
#                                           )
#     )
# 
#     firstResult <- querySql(con, translatedSql)
# 
#     secondTime <- system.time(
# 
#     visitContextResult <- getVisitContext(connection = con,
#                                           cdmDatabaseSchema = server$cdmDatabaseSchema,
#                                           tempEmulationSchema = server$tempEmulationSchema,
#                                           cohortDatabaseSchema = server$cohortDatabaseSchema,
#                                           cohortTable =  server$cohortTable,
#                                           cohortIds = server$cohortIds,
#                                           conceptIdTable = "#concept_ids",
#                                           cdmVersion = 5
#                                           )
#     )
# 
#     secondResult <- querySql(con, translatedSql)
# 
#     expect_equal(firstResult, secondResult)
# 
#   })
#   
#   
#   # For testing the runVisitContext, there is no need to run it on multiple database systems as no sql other than
#   # the one included in the getVisitContext is executed.
#   if (nm == "sqlite"){
# 
#     test_that(paste("test that when incremental is FALSE the incremental file is not generated"), {
#   
#       expect_false(file.exists(file.path(exportFolder,"incremental")))
#   
#       runVisitContext(connection = con,
#                       cohortDefinitionSet = server$cohortDefinitionSet,
#                       exportFolder = exportFolder,
#                       databaseId = nm ,
#                       cohortDatabaseSchema = server$cohortDatabaseSchema,
#                       cdmDatabaseSchema = server$cdmDatabaseSchema,
#                       minCellCount = 0,
#                       incremental = FALSE
#                       )
#   
#       expect_false(file.exists(file.path(exportFolder,"incremental")))
#     })
# 
#     test_that(paste("test that when incremental is TRUE the incremental file is generated when it doesn't exist"), {
# 
#       expect_false(file.exists(file.path(exportFolder, "incremental")))
# 
#       runVisitContext(connection = con,
#                       cohortDefinitionSet = server$cohortDefinitionSet,
#                       exportFolder = exportFolder,
#                       databaseId = nm ,
#                       cohortDatabaseSchema = server$cohortDatabaseSchema,
#                       cdmDatabaseSchema = server$cdmDatabaseSchema,
#                       minCellCount = 0,
#                       incremental = TRUE
#                       )
# 
#       expect_true(file.exists(file.path(exportFolder, "incremental")))
# 
#       })
# 
# 
#     test_that(paste("test that the output file visit_context.csv is generated and is identical with the output of getVisitContext()"), {
# 
#       getVisitContextResult <- getVisitContext(connection = con,
#                                                cdmDatabaseSchema = server$cdmDatabaseSchema,
#                                                tempEmulationSchema = server$tempEmulationSchema,
#                                                cohortDatabaseSchema = server$cohortDatabaseSchema,
#                                                cohortTable =  server$cohortTable,
#                                                cohortIds = server$cohortIds,
#                                                conceptIdTable = "#concept_ids",
#                                                cdmVersion = 5
#                                                )
#       
#       getVisitContextResult <- unname(getVisitContextResult)
#       
#       runVisitContext(connection = con,
#                        cohortDefinitionSet = server$cohortDefinitionSet,
#                        exportFolder = exportFolder,
#                        databaseId = nm,
#                        cohortTable =  server$cohortTable,
#                        cohortDatabaseSchema = server$cohortDatabaseSchema,
#                        cdmDatabaseSchema = server$cdmDatabaseSchema,
#                        minCellCount = 0,
#                        incremental = FALSE
#                        
#                         )
#       resultCsv <- file.path(exportFolder, "visit_context.csv")
# 
#       expect_true(file.exists(resultCsv))
#       
#       runVisitContextResult <- read.csv(resultCsv, header = TRUE, sep = ",")
#       runVisitContextResult$database_id <- NULL
#       runVisitContextResult <- unname(runVisitContextResult)
#       
#       expect_equal(getVisitContextResult, runVisitContextResult)
# 
#     })
# 
#   }
# }


###### Test cases with custom data

test_that(paste("test that the subject counts per cohort, visit concept and visit context are correct"), {
  
  cohortDataFilePath <- system.file("test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_cohort.xlsx",  package = "CohortDiagnostics")

  patientDataFilePath <- "test_cases/runVisitContext/testSubjectCounts/test_getVisitContext_patientData.json"
    
  connectionDetailsCustomCDM <- createCustomCdm(patientDataFilePath)

  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetailsCustomCDM)
  
  # add the cohort table
  cohortTableData <- readxl::read_excel(cohortDataFilePath, col_types = c("numeric", "numeric", "date", 
                                                                          "date"))
  
  cohortTableData <- cohortTableData %>% mutate(across(ends_with("DATE"), as.Date, format = "%Y-%m-%d"))
  
  # add the cohort table
  DatabaseConnector::insertTable(connection = connection, 
                                 tableName = "cohort",
                                 data = cohortTableData
                                 )
  
  
  visitContextResult <- getVisitContext(connection = connection,
                                        cdmDatabaseSchema = "main",
                                        tempEmulationSchema = "main",
                                        cohortDatabaseSchema = "main",
                                        cohortTable =  "cohort",
                                        cohortIds = list(1,2),
                                        conceptIdTable = "#concept_ids",
                                        cdmVersion = 5
  )
  
  resultPath <- system.file("test_cases/runVisitContext/testSubjectCounts/expectedResult.xlsx", package = "CohortDiagnostics")
  
  resultData <- readxl::read_excel(resultPath, col_types = c("numeric", "numeric", "text", 
                                                             "numeric"))
  
  are_equal <- all(sort(unlist(visitContextResult)) == sort(unlist(resultData)))
  
  expect_true(are_equal)
  
})







