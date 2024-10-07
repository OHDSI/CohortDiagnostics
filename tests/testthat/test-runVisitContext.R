library(SqlRender)


for (nm in names(testServers)) {
  
  # print(nm)
  
  server <- testServers[[nm]]
  
  con <- connect(server$connectionDetails)
  
  exportFolder <- tempfile()
  dir.create(exportFolder)
    
  test_that(paste("test no duplicates in concept_ids table for getVisitContext function", nm), { 
    
    sql <-  "SELECT * FROM #concept_ids"
    
    
    translatedSql <- translate(sql, targetDialect = server$connectionDetails$dbms)
    
    
    firstTime <- system.time(
    
    
    visitContextResult <- getVisitContext(connectionDetails = server$connectionDetails,
                    connection = con,
                    cdmDatabaseSchema = server$cdmDatabaseSchema,
                    tempEmulationSchema = server$tempEmulationSchema,
                    cohortDatabaseSchema = server$cohortDatabaseSchema,
                    cohortTable =  server$cohortTable,
                    cohortIds = server$cohortIds,
                    conceptIdTable = "#concept_ids",
                    cdmVersion = 5
    )
    )
    
    firstResult <- querySql(con, translatedSql)
    
    secondTime <- system.time(
      
      
      visitContextResult <- getVisitContext(connectionDetails = server$connectionDetails,
                                            connection = con,
                                            cdmDatabaseSchema = server$cdmDatabaseSchema,
                                            tempEmulationSchema = server$tempEmulationSchema,
                                            cohortDatabaseSchema = server$cohortDatabaseSchema,
                                            cohortTable =  server$cohortTable,
                                            cohortIds = server$cohortIds,
                                            conceptIdTable = "#concept_ids",
                                            cdmVersion = 5
      )
    )
    
    secondResult <- querySql(con, translatedSql)
    
    expect_equal(firstResult, secondResult)
  
  })
  
}

