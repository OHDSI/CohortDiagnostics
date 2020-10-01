#TODO: verify cdm vs results table and map to appropriate vector
# (will work fine as is if CDM and results tables in same schema)
resultsGlobalReferenceTables <- c(
  "analysisRef",
  "cohort",
  "cohortCount",
  "cohortOverlap",
  "conceptSets",
  "covariateRef",
  "database",
  "incidenceRate",
  "includedSourceConcept",
  "inclusionRuleStats",
  "indexEventBreakdown",
  "orphanConcept",
  "phenotypeDescription",
  "temporalAnalysisRef",
  "temporalCovariateRef",
  "temporalTimeRef",
  "timeDistribution"
)

cdmGlobalReferenceTables <- c(
  "concept",
  "conceptAncestor",
  "conceptRelationship",
  "domain",
  "vocabulary"
)



getResultsDatabaseSchema <- function(resultsSchemaEnvVar = "phenotypeLibraryDbResultsSchema",
                                     warnOnMissingVars = FALSE) {
  schema <- Sys.getenv(resultsSchemaEnvVar)
  if (schema == "" && warnOnMissingVars) {
    ParallelLogger::logWarn("Results schema environment variable missing")
  }
  return(ifelse(schema == "", NULL, schema))
}

getCdmDatabaseSchema <- function(cdmSchemaEnvVar = "phenotypeLibraryCdmSchema",
                                 warnOnMissingVars = FALSE) {
  schema <- Sys.getenv(cdmSchemaEnvVar)
  if (schema == "" && warnOnMissingVars) {
    ParallelLogger::logWarn("CDM schema environment variable missing")
  }
  return(ifelse(schema == "", NULL, schema))
} 

getDbConnectionDetails <- function(dbms = "postgresql",
                                  serverEnvVar = "phenotypeLibraryDbServer",
                                  portEnvVar = "phenotypeLibraryDbPort",
                                  databaseEnvVar = "phenotypeLibraryDbDatabase",
                                  userEnvVar = "phenotypeLibraryDbUser",
                                  pwdEnvVar = "phenotypeLibraryDbPassword",
                                  warnOnMissingVars = FALSE) {
  
  if (dbms != "postgresql") {
    warning("DBMS ", dbms, " not supported for Phenotype Library")
    return(NULL)
  }
  
  server <- Sys.getenv(serverEnvVar)
  port <- Sys.getenv(portEnvVar)
  database <- Sys.getenv(databaseEnvVar)
  user <- Sys.getenv(userEnvVar)
  pwd <- Sys.getenv(pwdEnvVar)
  
  dbVars <- c(server, port, database, user, pwd)
  
  if (any(is.null(dbVars))) {
    if (warnOnMissingVars) {
        ParallelLogger::logWarn("Environment variables missing for database connection")
    }
    return(NULL)
  }
  
  connDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms, 
                                                     server = paste(server,
                                                                    database,
                                                                    sep = "/"), 
                                                     port = port, 
                                                     user = user, 
                                                     password = pwd)
  
  return(connDetails)
}


loadGlobalDataFromLocal <- function(localPath) {
  load(localPath)
}


loadListOfTables <- function(connection, databaseSchema, tableList,
                             verbose, limit = -1) {
  for (tableName in tableList) {
    
    tableName <- SqlRender::camelCaseToSnakeCase(tableName)
    tableExists <- DatabaseConnector::dbExistsTable(conn = connection,
                                                    name = tableName,
                                                    schema = resultsDatabaseSchema)
    if (!tableExists) {
      stop(sprintf("Required results reference table %s does not exists in schema %s",
                   tableName, resultsDatabaseSchema))
    }
    
    
    sql <- SqlRender::render(sql = getSelectAllStatement("databaseSchema",
                                                         "tableName", limit = limit), 
                             databaseSchema = databaseSchema, 
                             tableName = tableName)
    
    if (verbose) {
      ParallelLogger::logInfo(sprintf("loading table %s", tableName))
    }
    
    tableData <- queryDatabase(connection, sql)
    
    assign(x = SqlRender::snakeCaseToCamelCase(tableName),
           value = tableData, envir = .GlobalEnv)
  }
}

loadGlobalDataFromDatabase <- function(connection,
                                       resultsDatabaseSchema,
                                       cdmDatabaseSchema,
                                       verbose = FALSE) {
  
  if(!exists("resultsGlobalReferenceTables")) {
    stop("No results global reference tables defined.")
  }
  if (!exists("cdmGlobalReferenceTables")) {
    stop("No CDM global reference tables defined.")
  }
  
  
  loadListOfTables(connection,
                   resultsDatabaseSchema,
                   resultsGlobalReferenceTables,
                   verbose)
  
  loadListOfTables(connection,
                   cdmDatabaseSchema,
                   cdmGlobalReferenceTables,
                   verbose)
  
  
}


getAllTablesInSchema <- function(connection, schemaName) {
  
  sql <- "SELECT table_name FROM information_schema.tables WHERE table_schema = '@schemaName' order by 1;"
  
  sql <- SqlRender::render(sql, schemaName = schemaName)
  
  resultSet <- queryDatabase(connection, sql)
  
  return(resultSet$tableName)
  
}


# needed for Shiny UI elements to render
instantiateEmptyTableObjects <- function(connection, resultsDatabaseSchema,
                                        cdmDatabaseSchema) {
  
  allTables <- union(getAllTablesInSchema(connection, resultsDatabaseSchema),
                     getAllTablesInSchema(connection, cdmDatabaseSchema))
  
  invisible(
    lapply(setdiff(allTables, union(resultsGlobalReferenceTables,
                                    cdmGlobalReferenceTables)),
           FUN = function(x) {
             assign(SqlRender::snakeCaseToCamelCase(x), tidyr::tibble())
           })
  )
}
