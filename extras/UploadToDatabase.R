# Using the official uploading functions to get data from zip files into the postgres database
library(CohortDiagnostics)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(Sys.getenv("phenotypeLibraryDbServer"),
                                                            Sys.getenv("phenotypeLibraryDbDatabase"),
                                                            sep = "/"),
                                             port = Sys.getenv("phenotypeLibraryDbPort"),
                                             user = Sys.getenv("phenotypeLibraryDbUser"),
                                             password = Sys.getenv("phenotypeLibraryDbPassword"))
schema <- Sys.getenv("phenotypeLibraryDbSchema")

createResultsDataModel(connectionDetails = connectionDetails, schema = schema)
uploadResults(connectionDetails = connectionDetails,
              schema = schema,
              zipFileName = "S:/examplePackageOutput/CCAE/diagnosticsExport/Results_CCAE.zip")



uploadResults(connectionDetails = connectionDetails,
              schema = schema,
              zipFileName = "s:/immunology/Results_JMDC.zip")

uploadResults(connectionDetails = connectionDetails,
              schema = schema,
              zipFileName = "s:/immunology/Results_OPTUM_PANTHER.zip")
