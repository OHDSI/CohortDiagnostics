# Using the official uploading functions to get data from zip files into the postgres database
library(CohortDiagnostics)
portNumber <- 5432

# OHDSI's server:
# connectionDetails <- createConnectionDetails(
#   dbms = "postgresql",
#   server = paste(
#     Sys.getenv("shinydbServer"),
#     Sys.getenv("shinydbDatabase"),
#     sep = "/"
#   ),
#   port = portNumber,
#   user = Sys.getenv("shinydbUser"),
#   password = Sys.getenv("shinydbPW")
# )
resultsSchema <- 'phenotype_library'


# OHDSI's Phenotype library server:
# connectionDetails <- createConnectionDetails(
#   dbms = "postgresql",
#   server = paste(
#     Sys.getenv("phenotypeLibraryServer"),
#     Sys.getenv("phenotypeLibrarydb"),
#     sep = "/"
#   ),
#   port = portNumber,
#   user = Sys.getenv("phenotypeLibrarydbUser"),
#   password = Sys.getenv("phenotypeLibrarydbPw")
# )
# resultsSchema <- Sys.getenv("phenotypeLibrarydbTargetSchema")
# 

# other
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = paste(
    keyring::key_get("shinydbServer"),
    keyring::key_get("shinydbDatabase"),
    sep = "/"
  ),
  port = portNumber,
  user = keyring::key_get("shinydbUser"),
  password = keyring::key_get("shinydbPW")
)



# commenting this function as it maybe accidentally run - loosing data.
# DatabaseConnector::renderTranslateExecuteSql(connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#                                              sql = paste0("select create_schema('",
#                                                           resultsSchema,
#                                                           "');"))
# createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)


Sys.setenv("POSTGRES_PATH" = Sys.getenv('POSTGRES_PATH'))

folderWithZipFilesToUpload <- "D:\\studyResults\\ohdaPhenotypeLibraryP\\tempUpload"
listOfZipFilesToUpload <-
  list.files(
    path = folderWithZipFilesToUpload,
    pattern = ".zip",
    full.names = TRUE,
    recursive = TRUE
  )

for (i in (1:length(listOfZipFilesToUpload))) {
  CohortDiagnostics::uploadResults(
    connectionDetails = connectionDetails,
    schema = resultsSchema,
    zipFileName = listOfZipFilesToUpload[[i]]
  )
}

# uploadPrintFriendly was removed in version 2.1
# uploadPrintFriendly(connectionDetails = connectionDetails,
#                     schema = resultsSchema)

launchDiagnosticsExplorer(connectionDetails = connectionDetails,
                          resultsDatabaseSchema = resultsSchema)
