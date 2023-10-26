
loadShinySettings <- function(configPath) {
  stopifnot(file.exists(configPath))
  shinySettings <- yaml::read_yaml(configPath)

  defaultValues <- list(
    resultsDatabaseSchema = c("main"),
    vocabularyDatabaseSchemas = c("main"),
    tablePrefix = "",
    cohortTable = "cohort",
    databaseTable = "database",
    connectionEnvironmentVariables = NULL
  )

  for (key in names(defaultValues)) {
    if (is.null(shinySettings[[key]])) {
      shinySettings[[key]] <- defaultValues[[key]]
    }
  }

  if (shinySettings$cohortTableName == "cohort") {
    shinySettings$cohortTableName <- paste0(shinySettings$tablePrefix, shinySettings$cohortTableName)
  }

  if (shinySettings$databaseTableName == "database") {
    shinySettings$databaseTableName <- paste0(shinySettings$tablePrefix, shinySettings$databaseTableName)
  }

  if (!is.null(shinySettings$connectionDetailsSecureKey)) {
    shinySettings$connectionDetails <- jsonlite::fromJSON(keyring::key_get(shinySettings$connectionDetailsSecureKey))
  } else if(!is.null(shinySettings$connectionEnvironmentVariables$server)) {

    defaultValues <- list(
      dbms = "",
      user = "",
      password = "",
      port = "",
      extraSettings = ""
    )

    for (key in names(defaultValues)) {
      if (is.null(shinySettings$connectionEnvironmentVariables[[key]])) {
        shinySettings$connectionEnvironmentVariables[[key]] <- defaultValues[[key]]
      }
    }

    serverStr <- Sys.getenv(shinySettings$connectionEnvironmentVariables$server)
    if (!is.null(shinySettings$connectionEnvironmentVariables$database)) {
      serverStr <- paste0(serverStr, "/", Sys.getenv(shinySettings$connectionEnvironmentVariables$database))
    }

    shinySettings$connectionDetails <- list(
      dbms = Sys.getenv(shinySettings$connectionEnvironmentVariables$dbms, unset = shinySettings$connectionDetails$dbms),
      server = serverStr,
      user = Sys.getenv(shinySettings$connectionEnvironmentVariables$user),
      password = Sys.getenv(shinySettings$connectionEnvironmentVariables$password),
      port = Sys.getenv(shinySettings$connectionEnvironmentVariables$port, unset = shinySettings$connectionDetails$port),
      extraSettings = Sys.getenv(shinySettings$connectionEnvironmentVariables$extraSettings)
    )
  }
  shinySettings$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails,
                                             shinySettings$connectionDetails)

  return(shinySettings)
}


if (!exists("shinySettings")) {
  writeLines("Using settings provided by user")
  shinyConfigPath <- getOption("CD-shiny-config", default = "config.yml")
  shinySettings <- loadShinySettings(shinyConfigPath)
}

# Added to support publishing to posit connect and shinyapps.io (looks for a library or reauire)
if (FALSE) {
  require(RSQLite)
}

connectionHandler <- ResultModelManager::PooledConnectionHandler$new(shinySettings$connectionDetails)


if (packageVersion("OhdsiShinyModules") >= as.numeric_version("1.2.0")) {
  resultDatabaseSettings <- list(
    schema = shinySettings$resultsDatabaseSchema,
    vocabularyDatabaseSchema = shinySettings$vocabularyDatabaseSchema,
    cdTablePrefix = shinySettings$tablePrefix,
    cgTable = shinySettings$cohortTableName,
    databaseTable = shinySettings$databaseTableName
  )

  dataSource <-
    OhdsiShinyModules::createCdDatabaseDataSource(connectionHandler = connectionHandler,
                                                  resultDatabaseSettings = resultDatabaseSettings)
} else {
  dataSource <-
    OhdsiShinyModules::createCdDatabaseDataSource(
      connectionHandler = connectionHandler,
      schema = shinySettings$resultsDatabaseSchema,
      vocabularyDatabaseSchema = shinySettings$vocabularyDatabaseSchema,
      tablePrefix = shinySettings$tablePrefix,
      cohortTableName = shinySettings$cohortTableName,
      databaseTableName = shinySettings$databaseTableName
    )

}


