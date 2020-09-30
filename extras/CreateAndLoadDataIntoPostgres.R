library(magrittr)
source("extras/PostgresConfigurationVariables.R")

dbms <- "postgresql"
databaseSchema <- "diagnostics"
pathToSql <- file.path('inst', 'sql', 'postgres') #system.file('sql', 'sql_server', package = 'CohortDiagnostics')
sqlDropTableFile <- "postgres_ddl_results_data_model_drop.sql"
sqlCreateTableFile <- "postgres_ddl_results_data_model.sql"
sqlTableConstraintsfile <- "postgres_ddl_results_data_model_constraints.sql"

csvLocation = file.path("extras", "CSVFiles")

uploadCsvToDatabase <- function(file, folder, schema) {
  tableName <- stringr::str_replace(string = file, 
                                    pattern = ".csv$", 
                                    replacement = "")
  # if ("temporal_covariate_value" %in% tableName) {
  #   return()
  # }
  ParallelLogger::logInfo(paste("Uploading", tableName, sep = " "))
  # checking whether the table exists with the CSV file name
  if (DatabaseConnector::dbExistsTable(conn = connection, 
                                       name = tableName, 
                                       schema = schema)) {
    deleteTableContentsSql <- "DELETE from @databaseSchema.@table;"
    deleteTableContentsSql <- SqlRender::render(sql = deleteTableContentsSql, 
                                                table = tableName, 
                                                databaseSchema = schema)
    DatabaseConnector::executeSql(connection = connection, 
                                  sql = deleteTableContentsSql)
    
    csvData <- readr::read_csv(file = file.path(folder, file),
                                            col_types = readr::cols(),
                                            guess_max = min(1e7),
                                            locale = readr::locale(encoding = "UTF-8"))
    
    csvData <- as.data.frame(csvData)
    
    DatabaseConnector::dbWriteTable(conn = connection, name = paste(schema, tableName, sep = "."),
                                    value = csvData,
                                    overwrite = TRUE,
                                    append = FALSE)
    # loadTableContentsSql <- "copy @databaseSchema.@table FROM '@csvPath' DELIMITER ',' CSV HEADER"
    # loadTableContentsSql <- SqlRender::render(sql = loadTableContentsSql, 
    #                                           table = tableName, 
    #                                           databaseSchema = schema, 
    #                                           csvPath = file.path(folder, file))
    # DatabaseConnector::executeSql(connection = connection, 
    #                               sql = loadTableContentsSql)
    invisible(NULL)
  }
}

if (file.exists(pathToSql)) {
  # 1. Connect to Postgres
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms, 
                                                                  server = Sys.getenv("server"), 
                                                                  port = Sys.getenv("port"), 
                                                                  user = Sys.getenv("user"), 
                                                                  password = Sys.getenv("password"))
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  csvFiles <- list.files(path = csvLocation, pattern = ".csv")
  
  # 2. Insert new schema if not exists
  sql <- "CREATE SCHEMA IF NOT EXISTS @databaseSchema AUTHORIZATION @user;"
  sql <- SqlRender::render(sql = sql, 
                           databaseSchema = databaseSchema, 
                           user = Sys.getenv("user"))
  DatabaseConnector::renderTranslateExecuteSql(connection = connection, sql = sql)
  
  # 4. Executes Drop Table from ddl
  if (file.exists(file.path(pathToSql, sqlDropTableFile))) {
    ParallelLogger::logInfo("Dropping Old Tables")
    sql <- SqlRender::readSql(sourceFile = file.path(pathToSql, sqlDropTableFile))
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 databaseSchema = databaseSchema,
                                                 sql = sql)
    flag <- "dropped"
  }
  
  # 5. Executes Create Table from ddl
  if (file.exists(file.path(pathToSql, sqlCreateTableFile)) && flag == "dropped") {
    ParallelLogger::logInfo("Creating the New Tables")
    sql <- SqlRender::readSql(sourceFile = file.path(pathToSql, sqlCreateTableFile))
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 databaseSchema = databaseSchema,
                                                 sql = sql)
    flag <- "created"
  }
  
  # 6. Upload values from CSV to Database 
  if (flag == "created") {
    lapply(X = csvFiles, 
           FUN = uploadCsvToDatabase, 
           folder = csvLocation, 
           schema = databaseSchema)
    invisible(NULL)
    flag <- "uploaded"
  }
  
  # 7. Adding constraints to the table comes next
  if (file.exists(file.path(pathToSql, sqlTableConstraintsfile)) && flag == "uploaded") {
    ParallelLogger::logInfo("Adding constraints Tables")
    sql <- SqlRender::readSql(sourceFile = file.path(pathToSql, sqlTableConstraintsfile))
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 databaseSchema = databaseSchema,
                                                 sql = sql)
    flag <- "finished"
  }
}
