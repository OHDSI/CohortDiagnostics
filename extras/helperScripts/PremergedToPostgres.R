# A simple script for uploading data in a premerged file to a Postgres database for testing
library(DatabaseConnector)

# Inputs: connection details, schema name and location of premerged:

# Martijn's local server;
# connectionDetails <- createConnectionDetails(dbms = "postgresql",
#                                              server = "localhost/ohdsi",
#                                              user = "postgres",
#                                              password = Sys.getenv("pwPostgres"))
# schema <- "phenotype_library"

connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(Sys.getenv("phenotypeLibraryDbServer"),
                                                            Sys.getenv("phenotypeLibraryDbDatabase"),
                                                            sep = "/"),
                                             port = Sys.getenv("phenotypeLibraryDbPort"),
                                             user = Sys.getenv("phenotypeLibraryDbUser"),
                                             password = Sys.getenv("phenotypeLibraryDbPassword"))
schema <- Sys.getenv("phenotypeLibraryDbSchema")

preMergedFile <- "S:/examplePackageOutput/diagnosticsExport/PreMerged.RData"

# No changes below this point
env <- new.env()
load(preMergedFile, envir = env)
connection <- connect(connectionDetails)
for (tableName in ls(env)) {
  sqlTableName <- SqlRender::camelCaseToSnakeCase(tableName)
  writeLines(paste("Inserting table ", sqlTableName))
  table <- get(tableName, env)
  
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = paste(schema, sqlTableName, sep = "."),
                                 data = table,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = FALSE,
                                 progressBar = TRUE,
                                 camelCaseToSnakeCase = TRUE)
}
disconnect(connection)
