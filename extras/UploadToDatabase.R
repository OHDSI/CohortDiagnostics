# Using the official uploading functions to get data from zip files into the postgres database
library(CohortDiagnostics)

# local server:
# connectionDetails <- createConnectionDetails(dbms = "postgresql",
#                                              server = "localhost/ohdsi",
#                                              user = "postgres",
#                                              password = Sys.getenv("pwPostgres"))
# resultsSchema <- "phenotype_library"

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
path = ""
zipFilesToUpload <- list.files(path = path, 
           pattern = ".zip", 
           recursive = TRUE, 
           full.names = TRUE)

for (i in (1:length(zipFilesToUpload))) {
  uploadResults(connectionDetails = connectionDetails,
                schema = resultsSchema,
                zipFileName = zipFilesToUpload[[i]])
}

uploadPrintFriendly(connectionDetails = connectionDetails,
                    schema = resultsSchema)

launchDiagnosticsExplorer(connectionDetails = connectionDetails,
                          resultsDatabaseSchema = resultsSchema)
