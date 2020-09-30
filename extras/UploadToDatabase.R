# Using the official uploading functions to get data from zip files into the postgres database

connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "localhost/ohdsi",
                                             user = "postgres",
                                             password = Sys.getenv("pwPostgres"))
schema <- "phenotype_library"

createResultsDataModel(connectionDetails = connectionDetails, schema = schema)
uploadResults(connectionDetails = connectionDetails,
              schema = schema,
              zipFileName = "S:/examplePackageOutput/CCAE/diagnosticsExport/Results_CCAE.zip")
