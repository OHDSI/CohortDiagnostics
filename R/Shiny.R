# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Launch the Diagnostics Explorer Shiny app
#' @param connectionDetails An object of type \code{connectionDetails} as created using the
#'                          \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                          DatabaseConnector package, specifying how to connect to the server where
#'                          the CohortDiagnostics results have been uploaded using the
#'                          \code{\link{uploadResults}} function.
#' @param resultsDatabaseSchema  The schema on the database server where the CohortDiagnostics results
#'                               have been uploaded.
#' @param vocabularyDatabaseSchema (Deprecated) Please use vocabularyDatabaseSchemas.
#' @param vocabularyDatabaseSchemas  (optional) A list of one or more schemas on the database server where the vocabulary tables are located.
#'                                   The default value is the value of the resultsDatabaseSchema. We can provide a list of vocabulary schema
#'                                   that might represent different versions of the OMOP vocabulary tables. It allows us to compare the impact
#'                                   of vocabulary changes on Diagnostics. Not supported with an sqlite database.
#' @param sqliteDbPath     Path to merged sqlite file. See \code{\link{createMergedResultsFile}} to create file.
#' @param shinyConfigPath  Path to shiny yml configuration file (use instead of sqliteDbPath or connectionDetails object)
#' @param runOverNetwork   (optional) Do you want the app to run over your network?
#' @param port             (optional) Only used if \code{runOverNetwork} = TRUE.
#' @param launch.browser   Should the app be launched in your default browser, or in a Shiny window.
#'                         Note: copying to clipboard will not work in a Shiny window.
#' @param aboutText        Text (using HTML markup) that will be displayed in an About tab in the Shiny app.
#'                         If not provided, no About tab will be shown.
#' @param tablePrefix      (Optional)  string to insert before table names (e.g. "cd_") for database table names
#' @param cohortTableName  (Optional) if cohort table name differs from the standard - cohort (ignores prefix if set)
#' @param databaseTableName (Optional) if database table name differs from the standard - database (ignores prefix if set)
#'
#' @param makePublishable (Optional) copy data files to make app publishable to posit connect/shinyapp.io
#' @param publishDir      If make publishable is true - the directory that the shiny app is copied to
#' @param overwritePublishDir      (Optional) If make publishable is true - overwrite the directory for publishing
#'
#' @details
#' Launches a Shiny app that allows the user to explore the diagnostics
#'
#' @export
launchDiagnosticsExplorer <- function(sqliteDbPath = "MergedCohortDiagnosticsData.sqlite",
                                      connectionDetails = NULL,
                                      shinyConfigPath = NULL,
                                      resultsDatabaseSchema = NULL,
                                      vocabularyDatabaseSchema = NULL,
                                      vocabularyDatabaseSchemas = resultsDatabaseSchema,
                                      tablePrefix = "",
                                      cohortTableName = "cohort",
                                      databaseTableName = "database",
                                      aboutText = NULL,
                                      runOverNetwork = FALSE,
                                      port = 80,
                                      makePublishable = FALSE,
                                      publishDir = file.path(getwd(), "DiagnosticsExplorer"),
                                      overwritePublishDir = FALSE,
                                      launch.browser = FALSE) {
  useShinyPublishFile <- FALSE
  if (is.null(shinyConfigPath)) {
    if (is.null(connectionDetails)) {
      sqliteDbPath <- normalizePath(sqliteDbPath)
      if (!file.exists(sqliteDbPath)) {
        stop("Sqlite database", sqliteDbPath, "not found. Please see createMergedSqliteResults")
      }

      resultsDatabaseSchema <- "main"
      vocabularyDatabaseSchemas <- "main"
      connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
      useShinyPublishFile <- TRUE
    }

    if (is.null(resultsDatabaseSchema)) {
      stop("resultsDatabaseSchema is required to connect to the database.")
    }
    if (!is.null(vocabularyDatabaseSchema) &
      is.null(vocabularyDatabaseSchemas)) {
      vocabularyDatabaseSchemas <- vocabularyDatabaseSchema
      warning(
        "vocabularyDatabaseSchema option is deprecated. Please use vocabularyDatabaseSchemas."
      )
    }

    if (cohortTableName == "cohort") {
      cohortTableName <- paste0(tablePrefix, cohortTableName)
    }

    if (databaseTableName == "database") {
      databaseTableName <- paste0(tablePrefix, databaseTableName)
    }

    .GlobalEnv$shinySettings <- list(
      connectionDetails = connectionDetails,
      resultsDatabaseSchema = resultsDatabaseSchema,
      vocabularyDatabaseSchemas = vocabularyDatabaseSchemas,
      aboutText = aboutText,
      tablePrefix = tablePrefix,
      cohortTableName = cohortTableName,
      databaseTableName = databaseTableName,
      enableAuthorization = FALSE
    )
    on.exit(rm("shinySettings", envir = .GlobalEnv))
  } else {
    checkmate::assertFileExists(shinyConfigPath)
    options("CD-shiny-config" = normalizePath(shinyConfigPath))
    on.exit(options("CD-shiny-config" = NULL))
  }

  if (!"OhdsiShinyModules" %in% as.data.frame(utils::installed.packages())$Package) {
    remotes::install_github("OHDSI/OhdsiShinyModules")
  }

  appDir <-
    system.file("shiny", "DiagnosticsExplorer", package = utils::packageName())

  if (makePublishable) {
    if (dir.exists(publishDir) && !overwritePublishDir) {
      warning("Directory for publishing exists, use overwritePublishDir to overwrite")
    } else {
      if (getwd() == publishDir) {
        stop("Publishable dir should not be current working directory")
      }

      dir.create(publishDir, showWarnings = FALSE)
      filesToCopy <- list.files(appDir, all.files = TRUE, full.names = TRUE)
      file.copy(filesToCopy, publishDir, recursive = TRUE, overwrite = TRUE)
      if (useShinyPublishFile) {
        file.copy(sqliteDbPath, file.path(publishDir, "data", "MergedCohortDiagnosticsData.sqlite"), overwrite = TRUE)
      } else if (is.null(shinyConfigPath)) {
        stop("Cannot make publishable shiny app when using connectionDetails object. Please create a config file")
      } else {
        file.copy(shinyConfigPath, file.path(publishDir, "config.yml"))
      }
    }
    appDir <- publishDir
  }

  if (launch.browser) {
    options(shiny.launch.browser = TRUE)
  }

  if (runOverNetwork) {
    options(shiny.port = port)
    options(shiny.host = "0.0.0.0")
  }

  shiny::runApp(appDir = appDir)
}

#' Merge Shiny diagnostics files into sqlite database
#'
#' @description
#' This function combines diagnostics results from one or more databases into a single file. The result is an
#' sqlite database that can be used as input for the Diagnostics Explorer Shiny app.
#'
#' It also checks whether the results conform to the results data model specifications.
#'
#' @param dataFolder       folder where the exported zip files for the diagnostics are stored. Use
#'                         the \code{\link{executeDiagnostics}} function to generate these zip files.
#'                         Zip files containing results from multiple databases may be placed in the same
#'                         folder.
#' @param sqliteDbPath     Output path where sqlite database is placed
#' @param overwrite        (Optional) overwrite existing sqlite lite db if it exists.
#' @param tablePrefix      (Optional) string to insert before table names (e.g. "cd_") for database table names
#' @export
createMergedResultsFile <-
  function(dataFolder,
           sqliteDbPath = "MergedCohortDiagnosticsData.sqlite",
           overwrite = FALSE,
           tablePrefix = "") {
    if (file.exists(sqliteDbPath) & !overwrite) {
      stop("File ", sqliteDbPath, " already exists. Set overwrite = TRUE to replace")
    } else if (file.exists(sqliteDbPath)) {
      unlink(sqliteDbPath)
    }

    connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    createResultsDataModel(
      connectionDetails = connectionDetails,
      databaseSchema = "main",
      tablePrefix = tablePrefix
    )
    listOfZipFilesToUpload <-
      list.files(
        path = dataFolder,
        pattern = ".zip",
        all.files = TRUE,
        full.names = TRUE,
        recursive = TRUE
      )

    for (zipFileName in listOfZipFilesToUpload) {
      uploadResults(
        connectionDetails = connectionDetails,
        schema = "main",
        zipFileName = zipFileName,
        tablePrefix = tablePrefix
      )
    }
    DatabaseConnector::renderTranslateExecuteSql(connection, "VACUUM;")
  }

#' Create publishable shiny zip
#' @description
#' A utility designed for creating a published zip of a shiny app with an sqlite database.
#' Designed for sharing projects on servers like data.ohdsi.org.
#'
#' Takes the shiny code from the R project and adds an sqlite file to a zip archive.
#' Uncompressed cohort diagnostics sqlite databases can become large very quickly.
#'
#' @param outputZipfile         The output path for the zip file
#' @param sqliteDbPath          Merged Cohort Diagnostics sqlitedb created with \code{\link{createMergedResultsFile}}
#' @param shinyDirectory        (optional) Path to the location where the shiny code is stored. By default,
#'                              this is the package root
#' @param overwrite             If the zip file already exists, overwrite it?
#'
#' @export
createDiagnosticsExplorerZip <- function(outputZipfile = file.path(getwd(), "DiagnosticsExplorer.zip"),
                                         sqliteDbPath = "MergedCohortDiagnosticsData.sqlite",
                                         shinyDirectory = system.file(file.path("shiny", "DiagnosticsExplorer"),
                                                                      package = "CohortDiagnostics"
                                         ),
                                         overwrite = FALSE) {
  outputZipfile <- normalizePath(outputZipfile, mustWork = FALSE)

  if (file.exists(outputZipfile) & !overwrite) {
    stop(outputZipfile, " already exists. Set overwrite = TRUE to continue")
  }
  stopifnot(dir.exists(shinyDirectory))
  stopifnot(file.exists(sqliteDbPath))

  sqliteDbPath <- normalizePath(sqliteDbPath)

  message("Creating zip archive")

  tmpDir <- tempfile()
  dir.create(tmpDir)

  on.exit(unlink(tmpDir, recursive = TRUE, force = TRUE), add = TRUE)
  file.copy(shinyDirectory, tmpDir, recursive = TRUE)
  dir.create(file.path(tmpDir, "DiagnosticsExplorer", "data"))
  file.copy(sqliteDbPath, file.path(tmpDir, "DiagnosticsExplorer", "data", "MergedCohortDiagnosticsData.sqlite"))

  DatabaseConnector::createZipFile(outputZipfile, file.path(tmpDir, "DiagnosticsExplorer"), rootFolder = tmpDir)
}


#' Rsconnect deploy
#' @description
#' Deploy your application to an posit connect platform or shinyapps.io server
#'
#' @export
#' @inheritParams launchDiagnosticsExplorer
#' @param appName               string name to call app - should be unique on posit connect server
#' @param appDir                optional - directory to use to copy files for deployment. If you use a consistent dir
#'                              other internal options can change.
#' @param  useRenvironFile      logical - not recommended, store db credentials in .Renviron file
#' @param shinyDirectory        (optional) Directyory shiny app code lives. Use this if you wish to modify the explorer
#' @param ...                   other parameters passed to rsconnect::deployApp
deployPositConnectApp <- function(appName,
                                  appDir = tempfile(),
                                  sqliteDbPath = "MergedCohortDiagnosticsData.sqlite",
                                  shinyDirectory = system.file(file.path("shiny", "DiagnosticsExplorer"),
                                                               package = "CohortDiagnostics"
                                  ),
                                  connectionDetails = NULL,
                                  shinyConfigPath = NULL,
                                  resultsDatabaseSchema = NULL,
                                  vocabularyDatabaseSchemas = resultsDatabaseSchema,
                                  tablePrefix = "",
                                  cohortTableName = "cohort",
                                  databaseTableName = "database",
                                  port = 80,
                                  useRenvironFile = FALSE,
                                  ...) {

  if (!"rsconnect" %in% as.data.frame(utils::installed.packages())$Package) {
    install.packages("rsconnect")
  }

  if (!"yaml" %in% as.data.frame(utils::installed.packages())$Package) {
    install.packages("yaml")
  }

  if (!"OhdsiShinyModules" %in% as.data.frame(utils::installed.packages())$Package) {
    remotes::install_github("OHDSI/OhdsiShinyModules")
  }

  checkmate::assertDirectory(appDir, access = "w")

  args <- rlang::dots_list(...)
  args$envVars <- c(args$envVars, DATABASECONNECTOR_JAR_FOLDER = "./")

  dir.create(appDir, showWarnings = FALSE)
  filesToCopy <- list.files(shinyDirectory, all.files = TRUE, full.names = TRUE)
  file.copy(filesToCopy, appDir, recursive = TRUE, overwrite = TRUE)

  if (is.null(connectionDetails) && is.null(shinyConfigPath)) {
    checkmate::assertFileExists(sqliteDbPath)
    file.copy(sqliteDbPath, file.path(appDir, "data", "MergedCohortDiagnosticsData.sqlite"), overwrite = TRUE)
  } else if (!is.null(shinyConfigPath)) {

    DatabaseConnector::downloadJdbcDrivers(connectionDetails$dbms, appDir)
    file.copy(shinyConfigPath, file.path(appDir, "config.yml"))
  } else {
    DatabaseConnector::downloadJdbcDrivers(connectionDetails$dbms, appDir)
    if (useRenvironFile) {
      outputText <- "# Edit credentials here to set on remote server
# Using an renviron file will store plaintext variables and is not reccomended.
# A local copy of this will be created and deleted following app deployment
shinyDbServer=''
shinydbPw=''
shinydbUser=''
shinydbPort=5432
DATABASECONNECTOR_JAR_FOLDER='.'
"
      writeLines(outputText, file.path(appDir, ".Renviron"))
      res <- utils::edit(file=file.path(appDir, ".Renviron"))
      # File should always be deleted
      on.exit(unlink(file.path(appDir, ".Renviron"), force = TRUE))
    } else {
      args$envVars <- c(args$envVars,
                        shinyDbServer = connectionDetails$server(),
                        shinydbPw = connectionDetails$password(),
                        shinydbUser = connectionDetails$user(),
                        shinydbPort = connectionDetails$port())
    }

    configOpts <- yaml::read_yaml(file.path(appDir, "config-ohdsi-shiny.yml"))
    configOpts$tablePrefix <- tablePrefix
    configOpts$resultsDatabaseSchema <- resultsDatabaseSchema
    configOpts$vocabularyDatabaseSchemas <- vocabularyDatabaseSchemas
    configOpts$cohortTableName <- cohortTableName
    configOpts$databaseTableName <- databaseTableName
    yaml::write_yaml(configOpts, file.path(appDir, "config.yml"))
  }

  args$appDir <- appDir
  args$appName <- appName
  do.call(rsconnect::deployApp, args)
}