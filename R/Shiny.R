# Copyright 2020 Observational Health Data Sciences and Informatics
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
#'
#' @param dataFolder       A folder where the exported zip files for the diagnostics are stored. Use
#'                         the \code{\link{runCohortDiagnostics}} function to generate these zip files. 
#'                         Zip files containing results from multiple databases can be placed in the same
#'                         folder.
#' @param launch.browser   Should the app be launched in your default browser, or in a Shiny window.
#'                         Note: copying to clipboard will not work in a Shiny window.
#'
#' @details
#' Launches a Shiny app that allows the user to explore the diagnostics
#'
#' @export
launchDiagnosticsExplorer <- function(dataFolder, launch.browser = FALSE) {
  ensure_installed("shiny")
  ensure_installed("shinydashboard")
  ensure_installed("shinyWidgets")
  ensure_installed("DT")
  ensure_installed("VennDiagram")
  ensure_installed("htmltools")
  ensure_installed("scales")
  ensure_installed("plotly")
  
  appDir <- system.file("shiny", "DiagnosticsExplorer", package = "CohortDiagnostics")
  shinySettings <- list(dataFolder = dataFolder)
  .GlobalEnv$shinySettings <- shinySettings
  on.exit(rm("shinySettings", envir = .GlobalEnv))
  shiny::runApp(appDir)
}

#' Premerge Shiny diagnostics files
#' 
#' @description 
#' If there are many diagnostics files, starting the Shiny app may take a very long time. This function 
#' already does most of the preprocessing, increasing loading speed.
#' 
#' The merged data will be stored in the same folder, and will automatically be recognized by the Shiny app.
#'
#' @param dataFolder       folder where the exported zip files for the diagnostics are stored. Use
#'                         the \code{\link{runCohortDiagnostics}} function to generate these zip files. 
#'                         Zip files containing results from multiple databases may be placed in the same
#'                         folder. 
#' @param outputFolder     (optional) folder where the post processed files for the diagnostics are to be stored. 
#'                         These files may be used with the results viewer or may be uploaded into RDMS.
#'                         Note this has to be different from dataFolder. If not provided, then only
#'                         premerged file is generated in the dataFolder. If provided, and different
#'                         from dataFolder the output will include csv, zip.
#' @param minCovariateProportion  minimum value threshold for covariates to be included 
#'                                in premerged file (valid number (maybe decimal) between 0 to 1)                         
#' @export
preMergeDiagnosticsFiles <- function(dataFolder, 
                                     outputFolder = NULL,
                                     minCovariateProportion = 0) {
  
  output <- file.path(tempdir(), 'output')
  dir.create(path = output, showWarnings = FALSE, recursive = TRUE)
  
  postProcessDiagnosticsResultsFiles(dataFolder = dataFolder, 
                                     outputFolder = output)
  
  if (!is.null(outputFolder)) {
    if (dataFolder == outputFolder) {
      warning('dataFolder and outputFolder may not be the same')
      stop()
    }
    dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)
    file.copy(from = output, to = outputFolder, overwrite = TRUE, recursive = TRUE)
  } 
  
  csvFiles <- dplyr::tibble(fullName = list.files(path = output, 
                                                  pattern = ".csv", 
                                                  recursive = TRUE, 
                                                  full.names = TRUE, 
                                                  ignore.case = FALSE)) %>% 
    dplyr::mutate(tableName = stringr::str_replace(string = basename(.data$fullName),
                                                   pattern = ".csv",
                                                   replacement = "")) 
  
  newEnvironment <- new.env()
  for (i in 1:nrow(csvFiles)) {
    data <- readr::read_csv(file = csvFiles$fullName[[i]],
                            col_types = readr::cols(), 
                            guess_max = min(1e7), 
                            locale = readr::locale(encoding = "UTF-8"))
    if (csvFiles$tableName[[i]] %in% c('covariate_value', 'temporal_covariate_value')) {
      data <- data %>% 
        dplyr::filter(mean >= minCovariateProportion)
    }
    colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
    if (length(data > 0)) {
      assign(x = SqlRender::snakeCaseToCamelCase(csvFiles$tableName[[i]]), 
             value = data, 
             envir = newEnvironment)
    } else {
      warning(paste0(csvFiles$tableName[[i]], " had 0 rows"))
    }
  }
  ParallelLogger::logInfo("Creating PreMerged.Rdata file. This might take some time.")
  save(list = ls(newEnvironment),
       envir = newEnvironment,
       compress = TRUE,
       compression_level = 9,
       file = file.path(outputFolder, "PreMerged.RData"))
  ParallelLogger::logInfo("Merged data saved in ", file.path(dataFolder, "PreMerged.RData"))
  rm(ls(newEnvironment), envir = newEnvironment)
  unlink(x = output, recursive = TRUE, force = TRUE)
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
                                   cohortId = cohortId,
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