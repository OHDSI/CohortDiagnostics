globalReferenceTables <- c(
  "analysisRef",
  "cohort",
  "cohortCount",
  "cohortOverlap",
  "concept",
  "conceptAncestor",
  "conceptRelationship",
  "conceptSets",
  "covariateRef",
  "database",
  "domain",
  "incidenceRate",
  "includedSourceConcept",
  "inclusionRuleStats",
  "indexEventBreakdown",
  "orphanConcept",
  "phenotypeDescription",
  "temporalAnalysisRef",
  "temporalCovariateRef",
  "temporalTimeRef",
  "timeDistribution",
  "vocabulary"
)


getDbConnectionDetails <- function(dbms = "postgresql",
                                  serverVar = "PL_DB_SERVER",
                                  portVar = "PL_DB_PORT",
                                  userVar = "PL_DB_USER",
                                  pwdVar = "PL_DB_PWD",
                                  warnOnMissingVars = FALSE) {
  
  if (dbms != "postgresql") {
    warning("Database ", dbms, " not supported for Phenotype Library")
    return(NULL)
  }
  
  server <- Sys.getenv(serverVar)
  port <- Sys.getenv(portVar)
  user <- Sys.getenv(userVar)
  pwd <- Sys.getenv(pwdVar)
  
  dbVars <- c(server, port, user, pwd)
  
  if (any(is.null(dbVars))) {
    if (warnOnMissingVars) {
      for(var in dbVars) {
        if(var == "") {
          ParallelLogger::logWarn(sprintf("Environment variable %s not defined",
                                          var))
        }
      }
    }
    return(NULL)
  }
  
  connDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms, 
                                                     server = server, 
                                                     port = port, 
                                                     user = user, 
                                                     password = pwd)
  
  return(connDetails)
}


loadGlobalDataFromLocal <- function(localPath) {
  load(localPath)
}

loadGlobalDataFromDatabase <- function(connection,
                                       databaseSchema = "diagnostics",
                                       verbose = FALSE) {
  
  if(!exists("globalReferenceTables")) {
    stop("No global reference tables defined.")
  }
  
  for (tableName in globalReferenceTables) {
    # TODO: replace with DatabaseConnector::dbExistsTable
    tableName <- SqlRender::camelCaseToSnakeCase(tableName)
    sql <- "SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_schema = '@databaseSchema'
   AND    table_name   = '@tableName'
   );"
    
    if (verbose) {
      ParallelLogger::logInfo(sprintf("Checking if table %s exists", tableName))
    }
    
    sql <- SqlRender::render(sql = sql, 
                             databaseSchema = databaseSchema, 
                             tableName = tableName)
    tableExists <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                sql = sql)
    if (tableExists$EXISTS != "t") {
      stop(sprintf("Reference table %s does not exists in schema %s",
                   tableName, databaseSchema))
    }
    
    sql <- "SELECT * FROM @databaseSchema.@tableName;"
    
    sql <- SqlRender::render(sql = sql, 
                             databaseSchema = databaseSchema, 
                             tableName = tableName)
    
    if (verbose) {
      ParallelLogger::logInfo(sprintf("loading table %s", tableName))
    }
    refTableData <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                              sql = sql)
    
    
    colnames(refTableData) <- SqlRender::snakeCaseToCamelCase(colnames(refTableData))
    
    if (verbose) {
      ParallelLogger::logInfo(sprintf("saving table to global environment", tableName))
    }
    
    assign(x = SqlRender::snakeCaseToCamelCase(tableName),
           value = refTableData, envir = .GlobalEnv)
  }
}