# Using the official uploading functions to get data from zip files into the postgres database
library(CohortDiagnostics)

# Martijn's local server:
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "localhost/ohdsi",
                                             user = "postgres",
                                             password = Sys.getenv("pwPostgres"))
resultsSchema <- "phenotype_library"

# OHDSI's server:
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(Sys.getenv("phenotypeLibraryDbServer"),
                                                            Sys.getenv("phenotypeLibraryDbDatabase"),
                                                            sep = "/"),
                                             port = Sys.getenv("phenotypeLibraryDbPort"),
                                             user = Sys.getenv("phenotypeLibraryDbUser"),
                                             password = Sys.getenv("phenotypeLibraryDbPassword"))
resultsSchema <- Sys.getenv("phenotypeLibraryDbResultsSchema")

createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)



Sys.setenv("POSTGRES_PATH" = "C:/Program Files/PostgreSQL/11/bin")
uploadResults(connectionDetails = connectionDetails,
              schema = resultsSchema,
              zipFileName = "S:/examplePackageOutput/CCAE/diagnosticsExport/Results_CCAE.zip")

uploadResults(connectionDetails = connectionDetails,
              schema = resultsSchema,
              zipFileName = "S:/examplePackageOutput/MDCD/diagnosticsExport/Results_IBM_MDCD.zip")
launchDiagnosticsExplorer(connectionDetails = connectionDetails,
                          resultsDatabaseSchema = resultsSchema)


uploadResults(connectionDetails = connectionDetails,
              schema = resultsSchema,
              zipFileName = "s:/immunology/Results_JMDC.zip")

uploadResults(connectionDetails = connectionDetails,
              schema = resultsSchema,
              zipFileName = "s:/immunology/Results_OPTUM_PANTHER.zip")

