# fix for linux systems with weird rJava behaviour
if (is.null(getOption("java.parameters")))
    options(java.parameters = "-Xss100m")

# Ensure dplyr (pipe) is available when app is run from a copy (e.g. shinytest2)
if (!requireNamespace("dplyr", quietly = TRUE)) stop("dplyr is required")
suppressPackageStartupMessages(library("dplyr", character.only = TRUE))

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

# Path to HTML help (fallback when app run from a copy, e.g. shinytest2)
cdWwwPath <- system.file("cohort-diagnostics-www", package = "CohortDiagnostics")
if (cdWwwPath == "" && dir.exists(file.path(getwd(), "cohort-diagnostics-www"))) {
  cdWwwPath <- file.path(getwd(), "cohort-diagnostics-www")
}

# Source embedded cohort diagnostics modules (order: helpers, components, shared, views, main)
modulesDir <- system.file("shiny", "DiagnosticsExplorer", "modules", package = "CohortDiagnostics")
if (!dir.exists(modulesDir) && dir.exists("modules")) {
  # App run from a copy (e.g. shinytest2): use modules next to app dir
  modulesDir <- "modules"
}
if (dir.exists(modulesDir)) {
  moduleFiles <- c(
    "helpers-elements.R", "helpers-emptyPlotly.R", "helpers-getHelp.R", "helpers-logo.R", "helpers-migrations.R",
    "component-tableSelect.R", "components-data-viewer.R", "components-helpInfo.R", "components-inputselection.R", "components-largeTableViewer.R",
    "cohort-diagnostics-shared.R",
    "cohort-diagnostics-definition.R", "cohort-diagnostics-counts.R", "cohort-diagnostics-databaseInformation.R",
    "cohort-diagnostics-conceptsInDataSource.R", "cohort-diagnostics-orphanConcepts.R", "cohort-diagnostics-indexEventBreakdown.R",
    "cohort-diagnostics-visitContext.R", "cohort-diagnostics-cohort-overlap.R", "cohort-diagnostics-timeDistributions.R",
    "cohort-diagnostics-characterization.R", "cohort-diagnostics-compareCharacterization.R", "cohort-diagnostics-inclusionRules.R",
    "cohort-diagnostics-incidenceRates.R", "cohort-diagnostics-main-ui.R", "cohort-diagnostics-main.R"
  )
  for (f in moduleFiles) {
    fpath <- file.path(modulesDir, f)
    if (file.exists(fpath)) source(fpath, local = FALSE)
  }
}

resultDatabaseSettings <- list(
  schema = shinySettings$resultsDatabaseSchema,
  vocabularyDatabaseSchemas = shinySettings$vocabularyDatabaseSchemas,
  cdTablePrefix = shinySettings$tablePrefix,
  cgTable = shinySettings$cohortTableName,
  databaseTable = shinySettings$databaseTableName
)

# Paths to ref files (fallback to app dir when app run from a copy, e.g. shinytest2)
dataModelSpecPath <- system.file("cohort-diagnostics-ref", "resultsDataModelSpecification.csv", package = "CohortDiagnostics")
migrationsRefPath <- system.file("cohort-diagnostics-ref", "migrations.csv", package = "CohortDiagnostics")
if (dataModelSpecPath == "" && file.exists(file.path(getwd(), "cohort-diagnostics-ref", "resultsDataModelSpecification.csv"))) {
  dataModelSpecPath <- file.path(getwd(), "cohort-diagnostics-ref", "resultsDataModelSpecification.csv")
  migrationsRefPath <- file.path(getwd(), "cohort-diagnostics-ref", "migrations.csv")
}

dataSource <- createCdDatabaseDataSource(
  connectionHandler = connectionHandler,
  resultDatabaseSettings = resultDatabaseSettings,
  dataModelSpecificationsPath = dataModelSpecPath,
  dataMigrationsRef = migrationsRefPath
)















