# Using the official uploading functions to get data from zip files into the postgres database
library(CohortDiagnostics)

# OHDSI's server:
connectionDetails <- createConnectionDetails(
  dbms = Sys.getenv("shinydbDbms", unset = "postgresql"),
  server = paste(
    Sys.getenv("shinydbServer"),
    Sys.getenv("shinydbDatabase"),
    sep = "/"
  ),
  port = Sys.getenv("shinydbPort"),
  user = Sys.getenv("shinydbUser"),
  password = Sys.getenv("shinydbPW")
)
resultsSchema <- 'targetSchema'

# commenting this function as it maybe accidentally run - loosing data.
# CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)
# sqlGrant <-
#   paste0("grant select on all tables in schema ",
#          resultsSchema,
#          " to phenotypelibrary;")
# DatabaseConnector::renderTranslateExecuteSql(
#   connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
#   sql = sqlGrant
# )


Sys.setenv("POSTGRES_PATH" = Sys.getenv('POSTGRES_PATH'))

folderWithZipFilesToUpload <- "D:\\results"
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

CohortDiagnostics::launchDiagnosticsExplorer(connectionDetails = connectionDetails,
                                             resultsDatabaseSchema = resultsSchema)
