# Copyright 2022 Observational Health Data Sciences and Informatics
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
#' @param runOverNetwork   (optional) Do you want the app to run over your network?
#' @param port             (optional) Only used if \code{runOverNetwork} = TRUE.
#' @param launch.browser   Should the app be launched in your default browser, or in a Shiny window.
#'                         Note: copying to clipboard will not work in a Shiny window.
#' @param aboutText        Text (using HTML markup) that will be displayed in an About tab in the Shiny app.
#'                         If not provided, no About tab will be shown.
#' @param enableAnnotation (optional) Boolean - Enable users to annotate cohorts.
#'                         Note, this is not recommended outside of an organisational firewall.
#'                         Default is to only use with an sqlite database.
#'
#' @details
#' Launches a Shiny app that allows the user to explore the diagnostics
#'
#' @export
launchDiagnosticsExplorer <- function(sqliteDbPath = "MergedCohortDiagnosticsData.sqlite",
                                      connectionDetails = NULL,
                                      resultsDatabaseSchema = NULL,
                                      vocabularyDatabaseSchema = NULL,
                                      vocabularyDatabaseSchemas = resultsDatabaseSchema,
                                      aboutText = NULL,
                                      runOverNetwork = FALSE,
                                      port = 80,
                                      launch.browser = FALSE,
                                      enableAnnotation = is.null(connectionDetails)) {
  sqliteDbPath <- normalizePath(sqliteDbPath)
  if (is.null(connectionDetails)) {
    if (!file.exists(sqliteDbPath)) {
      stop("Sqlite database", sqliteDbPath, "not found. Please see createMergedSqliteResults")
    }

    resultsDatabaseSchema <- "main"
    vocabularyDatabaseSchemas <- "main"
    connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
  }

  if (enableAnnotation) {
    message("Starting application with annotations enabled")
  }

  if (connectionDetails$dbms != "sqlite" & enableAnnotation) {
    warning("Enabling annotation is not currently recommended outside of sqlite databases")
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

  ensure_installed("checkmate")
  ensure_installed("DatabaseConnector")
  ensure_installed("dplyr")
  ensure_installed("plyr")
  ensure_installed("ggplot2")
  ensure_installed("ggiraph")
  ensure_installed("gtable")
  ensure_installed("htmltools")
  ensure_installed("lubridate")
  ensure_installed("pool")
  ensure_installed("purrr")
  ensure_installed("scales")
  ensure_installed("shiny")
  ensure_installed("shinydashboard")
  ensure_installed("shinyWidgets")
  ensure_installed("shinyjs")
  ensure_installed("shinycssloaders")
  ensure_installed("stringr")
  ensure_installed("SqlRender")
  ensure_installed("tidyr")
  ensure_installed("CirceR")
  ensure_installed("rmarkdown")
  ensure_installed("reactable")
  ensure_installed("markdownInput")
  ensure_installed("markdown")

  appDir <-
    system.file("shiny", "DiagnosticsExplorer", package = utils::packageName())

  if (launch.browser) {
    options(shiny.launch.browser = TRUE)
  }

  if (runOverNetwork) {
    myIpAddress <- system("ipconfig", intern = TRUE)
    myIpAddress <- myIpAddress[grep("IPv4", myIpAddress)]
    myIpAddress <- gsub(".*? ([[:digit:]])", "\\1", myIpAddress)
    options(shiny.port = port)
    options(shiny.host = myIpAddress)
  }
  .GlobalEnv$shinySettings <- list(
    connectionDetails = connectionDetails,
    resultsDatabaseSchema = resultsDatabaseSchema,
    vocabularyDatabaseSchemas = vocabularyDatabaseSchemas,
    aboutText = aboutText,
    enableAnnotation = enableAnnotation
  )
  on.exit(rm("shinySettings", envir = .GlobalEnv))
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
#' @export
createMergedResultsFile <-
  function(dataFolder,
           sqliteDbPath = "MergedCohortDiagnosticsData.sqlite",
           overwrite = FALSE) {
    if (file.exists(sqliteDbPath) & !overwrite) {
      stop("File ", sqliteDbPath, " already exists. Set overwrite = TRUE to replace")
    } else if (file.exists(sqliteDbPath)) {
      unlink(sqliteDbPath)
    }

    connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    createResultsDataModel(
      connection = connection,
      schema = "main"
    )
    listOfZipFilesToUpload <-
      list.files(
        path = dataFolder,
        pattern = ".zip",
        full.names = TRUE,
        recursive = TRUE
      )

    for (zipFileName in listOfZipFilesToUpload) {
      uploadResults(
        connectionDetails = connectionDetails,
        schema = "main",
        zipFileName = zipFileName
      )
    }
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


#' Launch the CohortExplorer Shiny app
#'
#' @template CohortTable
#'
#' @template CdmDatabaseSchema
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cohortId             The ID of the cohort.
#' @param sampleSize           Number of subjects to sample from the cohort. Ignored if subjectIds is specified.
#' @param subjectIds           A vector of subject IDs to view.
#'
#' @details
#' Launches a Shiny app that allows the user to explore a cohort of interest.
#'
#' @export
launchCohortExplorer <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 cohortDatabaseSchema,
                                 cohortTable,
                                 cohortId,
                                 sampleSize = 100,
                                 subjectIds = NULL) {
  ensure_installed("shiny")
  ensure_installed("DT")
  ensure_installed("plotly")
  ensure_installed("RColorBrewer")
  ensure_installed("ggplot2")
  ensure_installed("magrittr")

  .GlobalEnv$shinySettings <-
    list(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionId = cohortId,
      sampleSize = sampleSize,
      subjectIds = subjectIds
    )
  on.exit(rm("shinySettings", envir = .GlobalEnv))
  appDir <-
    system.file("shiny", "CohortExplorer", package = utils::packageName())
  shiny::runApp(appDir)
}

# Borrowed from devtools:
# https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
is_installed <- function(pkg, version = 0) {
  installed_version <-
    tryCatch(
      utils::packageVersion(pkg),
      error = function(e) {
        NA
      }
    )
  !is.na(installed_version) && installed_version >= version
}

# Borrowed and adapted from devtools:
# https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
ensure_installed <- function(pkg) {
  if (!is_installed(pkg)) {
    msg <-
      paste0(sQuote(pkg), " must be installed for this functionality.")
    if (interactive()) {
      message(msg, "\nWould you like to install it?")
      if (menu(c("Yes", "No")) == 1) {
        if (pkg == "CirceR") {
          ensure_installed("remotes")
          message(msg, "\nInstalling from Github using remotes")
          remotes::install_github("OHDSI/CirceR")
        } else {
          install.packages(pkg)
        }
      } else {
        stop(msg, call. = FALSE)
      }
    } else {
      stop(msg, call. = FALSE)
    }
  }
}
