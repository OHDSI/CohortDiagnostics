# Using the official uploading functions to get data from zip files into the postgres database
library(CohortDiagnostics)

# Martijn's local server:
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
resultsSchema <- Sys.getenv("phenotypeLibraryDbResultsSchema")

createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)

Sys.setenv("POSTGRES_PATH" = "C:/Program Files/PostgreSQL/9.3/bin")
uploadResults(connectionDetails = connectionDetails,
              schema = schema,
              zipFileName = "S:/examplePackageOutput/CCAE/diagnosticsExport/Results_CCAE.zip")



uploadResults(connectionDetails = connectionDetails,
              schema = schema,
              zipFileName = "s:/immunology/Results_JMDC.zip")

uploadResults(connectionDetails = connectionDetails,
              schema = schema,
              zipFileName = "s:/immunology/Results_OPTUM_PANTHER.zip")

