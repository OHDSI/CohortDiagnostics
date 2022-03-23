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
                                      launch.browser = FALSE) {
  sqliteDbPath <- normalizePath(sqliteDbPath)
  if (is.null(connectionDetails)) {
    if (!file.exists(sqliteDbPath)) {
      stop("Sqlite database", sqliteDbPath, "not found. Please see createMergedSqliteResults")
    }

    resultsDatabaseSchema <- "main"
    vocabularyDatabaseSchemas <- "main"
    connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
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
  ensure_installed("DT")
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
  ensure_installed("stringr")
  ensure_installed("SqlRender")
  ensure_installed("tidyr")
  ensure_installed("CirceR")
  ensure_installed("rmarkdown")

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
  shinySettings <- list(
    connectionDetails = connectionDetails,
    resultsDatabaseSchema = resultsDatabaseSchema,
    vocabularyDatabaseSchemas = vocabularyDatabaseSchemas,
    aboutText = aboutText
  )
  .GlobalEnv$shinySettings <- shinySettings
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
