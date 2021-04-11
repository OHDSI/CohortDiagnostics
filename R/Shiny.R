# Copyright 2021 Observational Health Data Sciences and Informatics
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
#' @param vocabularyDatabaseSchema  The schema on the database server where the vocabulary tables are located.
#' @param dataFolder       A folder where the premerged file is stored. Use
#'                         the \code{\link{preMergeDiagnosticsFiles}} function to generate this file.
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
launchDiagnosticsExplorer <- function(dataFolder = "data", 
                                      dataFile = "PreMerged.RData",
                                      connectionDetails = NULL,
                                      resultsDatabaseSchema = NULL,
                                      vocabularyDatabaseSchema = resultsDatabaseSchema,
                                      aboutText = NULL,
                                      runOverNetwork = FALSE,
                                      port = 80,
                                      launch.browser = FALSE) {
  if (!is.null(connectionDetails) && connectionDetails$dbms != "postgresql") 
    stop("Shiny application can only run against a Postgres database")
  
  ensure_installed("shiny")
  ensure_installed("shinydashboard")
  ensure_installed("shinyWidgets")
  ensure_installed("DT")
  ensure_installed("htmltools")
  ensure_installed("scales")
  ensure_installed("pool")
  ensure_installed("dplyr")
  ensure_installed("tidyr")
  ensure_installed("ggplot2")
  ensure_installed("gtable")
  ensure_installed("checkmate")
  ensure_installed("ggiraph")
  ensure_installed("stringr")
  ensure_installed("SqlRender")
  ensure_installed("DatabaseConnector")

  appDir <- system.file("shiny", "DiagnosticsExplorer", package = "CohortDiagnostics")  
  
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
  shinySettings <- list(connectionDetails = connectionDetails,
                        resultsDatabaseSchema = resultsDatabaseSchema,
                        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                        dataFolder = dataFolder,
                        dataFile = dataFile,
                        aboutText = aboutText)
  .GlobalEnv$shinySettings <- shinySettings
  on.exit(rm("shinySettings", envir = .GlobalEnv))
  shiny::runApp(appDir = appDir)
}

#' Premerge Shiny diagnostics files
#' 
#' @description 
#' This function combines diagnostics results from one or more databases into a single file. The result is a
#' single file that can be used as input for the Diagnostics Explorer Shiny app.
#' 
#' It also checks whether the results conform to the results data model specifications.
#'
#' @param dataFolder       folder where the exported zip files for the diagnostics are stored. Use
#'                         the \code{\link{runCohortDiagnostics}} function to generate these zip files. 
#'                         Zip files containing results from multiple databases may be placed in the same
#'                         folder. 
#' @param tempFolder       A folder on the local file system where the zip files are extracted to. Will be cleaned
#'                         up when the function is finished. Can be used to specify a temp folder on a drive that
#'                         has sufficient space if the default system temp space is too limited.
#'                      
#' @export
preMergeDiagnosticsFiles <- function(dataFolder, tempFolder = tempdir()) {
  
  zipFiles <- dplyr::tibble(zipFile = list.files(dataFolder, pattern = ".zip", full.names = TRUE, recursive = TRUE),
                            unzipFolder = "")
  ParallelLogger::logInfo("Merging ", nrow(zipFiles), " zip files.")

  unzipMainFolder <- tempfile("unzipTempFolder", tmpdir = tempFolder)
  dir.create(path = unzipMainFolder, recursive = TRUE)
  on.exit(unlink(unzipMainFolder, recursive = TRUE))
  
  for (i in 1:nrow(zipFiles)) {
    ParallelLogger::logInfo("- Unzipping ", basename(zipFiles$zipFile[i]))
    unzipFolder <- file.path(unzipMainFolder, sub(".zip", "", basename(zipFiles$zipFile[i])))
    dir.create(unzipFolder)
    zip::unzip(zipFiles$zipFile[i], exdir = unzipFolder)
    zipFiles$unzipFolder[i] <- unzipFolder
  }
    
  specifications = getResultsDataModelSpecifications()
  
  # Storing output in an environment for now. If things get too big, we may want to write 
  # directly to CSV files for insertion into database:
  newEnvironment <- new.env()
  
  processTable <- function(tableName, env) {
    ParallelLogger::logInfo("Processing table ", tableName)
    csvFileName <- paste0(tableName, ".csv")
    data <- dplyr::tibble()
    for (i in 1:nrow(zipFiles)) {
      if (csvFileName %in% list.files(zipFiles$unzipFolder[i])) {
        newData <- readr::read_csv(file.path(zipFiles$unzipFolder[i], csvFileName),
                                   col_types = readr::cols(),
                                   guess_max = min(1e6))
        if (nrow(newData) > 0) {
          newData <- checkFixColumnNames(table = newData, 
                           tableName = tableName, 
                           zipFileName = zipFiles$zipFile[i],
                           specifications = specifications)
          newData <- checkAndFixDataTypes(table = newData, 
                                          tableName = tableName, 
                                          zipFileName = zipFiles$zipFile[i],
                                          specifications = specifications)
          newData <- checkAndFixDuplicateRows(table = newData, 
                                              tableName = tableName, 
                                              zipFileName = zipFiles$zipFile[i],
                                              specifications = specifications) 
          data <- appendNewRows(data = data, 
                                newData = newData, 
                                tableName = tableName, 
                                specifications = specifications)
          
        }
      }
    }
    if (nrow(data) == 0) {
      ParallelLogger::logInfo("- No data found for table ", tableName)
    } else {
      colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      assign(SqlRender::snakeCaseToCamelCase(tableName), data, envir = env)
    }
  }
  invisible(lapply(unique(specifications$tableName), processTable, env = newEnvironment))
  ParallelLogger::logInfo("Creating PreMerged.Rdata file. This might take some time.")
  save(list = ls(newEnvironment),
       envir = newEnvironment,
       compress = TRUE,
       compression_level = 2,
       file = file.path(dataFolder, "PreMerged.RData"))
  rm(list = ls(newEnvironment), envir = newEnvironment)
  ParallelLogger::logInfo("Merged data saved in ", file.path(dataFolder, "PreMerged.RData"))
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
  .GlobalEnv$shinySettings <- list(connectionDetails = connectionDetails,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTable = cohortTable,
                                   cohortDefinitionId = cohortId,
                                   sampleSize = sampleSize,
                                   subjectIds = subjectIds)
  on.exit(rm("shinySettings", envir = .GlobalEnv))
  appDir <- system.file("shiny", "CohortExplorer", package = "CohortDiagnostics")
  shiny::runApp(appDir)
}

# Borrowed from devtools:
# https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
is_installed <- function(pkg, version = 0) {
  installed_version <- tryCatch(utils::packageVersion(pkg), error = function(e) NA)
  !is.na(installed_version) && installed_version >= version
}

# Borrowed and adapted from devtools:
# https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
ensure_installed <- function(pkg) {
  if (!is_installed(pkg)) {
    msg <- paste0(sQuote(pkg), " must be installed for this functionality.")
    if (interactive()) {
      message(msg, "\nWould you like to install it?")
      if (menu(c("Yes", "No")) == 1) {
        install.packages(pkg)
      } else {
        stop(msg, call. = FALSE)
      }
    } else {
      stop(msg, call. = FALSE)
    }
  }
}
