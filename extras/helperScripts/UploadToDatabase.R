# Using the official uploading functions to get data from zip files into the postgres database
library(CohortDiagnostics)

# OHDSI's server:
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = paste(
    Sys.getenv("shinydbServer"),
    Sys.getenv("shinydbDatabase"),
    sep = "/"
  ),
  port = Sys.getenv("shinydbPort"),
  user = Sys.getenv("shinydbUser"),
  password = Sys.getenv("shinydbPW")
)
# 
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = paste(
    keyring::key_get("shinydbServer"),
    keyring::key_get("shinydbDatabase"),
    sep = "/"
  ),
  port = keyring::key_get("shinydbPort"),
  user = keyring::key_get("shinydbUser"),
  password = keyring::key_get("shinydbPW")
)
resultsSchema <- 'ohdsi2021Reproducibility'

# commenting this function as it maybe accidentally run - loosing data.
# DatabaseConnector::renderTranslateExecuteSql(connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#                                              sql = paste0("select create_schema('",
#                                                           resultsSchema,
#                                                           "');"))
# createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)


Sys.setenv("POSTGRES_PATH" = Sys.getenv('POSTGRES_PATH'))

folderWithZipFilesToUpload <- "D:\\temp\\outputFolder\\packageMode"
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
