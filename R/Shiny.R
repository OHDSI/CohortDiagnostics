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
  appDir <- system.file("shiny", "DiagnosticsExplorer", package = "CohortDiagnostics")
  shinySettings <- list(dataFolder = dataFolder)
  .GlobalEnv$shinySettings <- shinySettings
  on.exit(rm(shinySettings, envir = .GlobalEnv))
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
#' @param dataFolder  folder where the exported zip files for the diagnostics are stored. Use
#'                         the \code{\link{runCohortDiagnostics}} function to generate these zip files. 
#'                         Zip files containing results from multiple databases can be placed in the same
#'                         folder. 
#' @param minCovariateProportion  minimum value threshold for covariates to be included 
#'                                in premerged file (valid number (maybe decimal) between 0 to 1)                         
#' @export
preMergeDiagnosticsFiles <- function(dataFolder, minCovariateProportion = 0) {
  zipFiles <- list.files(dataFolder, pattern = ".zip", full.names = TRUE)
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertNumber(x = minCovariateProportion, lower = 0, upper = 1, add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  loadFile <- function(file, folder, overwrite, minProportion = minProportion) {
    # print(file)
    tableName <- gsub(".csv$", "", file)
    camelCaseName <- SqlRender::snakeCaseToCamelCase(tableName)
    data <- readr::read_csv(file.path(folder, file), col_types = readr::cols(), guess_max = 1e7, locale = readr::locale(encoding = "UTF-8"))
    colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
    
    if (tableName %in% c('covariate_value', 'temporal_covariate_value')) {
      data <- data %>% 
        dplyr::filter(mean >= minCovariateProportion)
    }
    
    if (tableName %in% c('covariate','temporal_covariate')) {# this is a temporary solution as detailed here https://github.com/OHDSI/CohortDiagnostics/issues/162
      data2 <- data %>% 
        dplyr::group_by(.data$covariateId, .data$conceptId) %>% 
        dplyr::filter(.data$covariateName == max(.data$covariateName)) %>% 
        dplyr::slice(1) %>% 
        dplyr::ungroup()
      if (!nrow(data2) == nrow(data)) {
        ParallelLogger::logInfo('Warning: covariate found to have more than one record per 
                                covariateId, conceptId combination. The row record corresponding 
                                to maximum value of covariateName has been chosen. This led to reduction 
                                in number of rows from ', nrow(data), ' to ', nrow(data2))
        data <- data
      } else {data2 <- NULL}
    }
    
    if (!overwrite && exists(camelCaseName, envir = .GlobalEnv)) {
      existingData <- get(camelCaseName, envir = .GlobalEnv)
      if (nrow(existingData) > 0) {
        if (nrow(data) > 0) {
          if (all(colnames(existingData) %in% colnames(data)) &&
              all(colnames(data) %in% colnames(existingData))) {
            data <- data[, colnames(existingData)]
          } else {
            stop("Table columns do no match previously seen columns. Columns in ", 
                 file, 
                 ":\n", 
                 paste(colnames(data), collapse = ", "), 
                 "\nPrevious columns:\n",
                 paste(colnames(existingData), collapse = ", "))
            
          }
        }
      }
      data <- dplyr::bind_rows(existingData, data) %>% 
        dplyr::distinct()
    }
    assign(camelCaseName, data, envir = .GlobalEnv)
    
    invisible(NULL)
  }
  tableNames <- c()
  for (i in 1:length(zipFiles)) {
    writeLines(paste("Processing", zipFiles[i]))
    tempFolder <- tempfile()
    dir.create(tempFolder)
    unzip(zipFiles[i], exdir = tempFolder)
    
    csvFiles <- list.files(tempFolder, pattern = ".csv")
    tableNames <- c(tableNames, csvFiles)
    lapply(csvFiles, loadFile, folder = tempFolder, overwrite = (i == 1))
    
    unlink(tempFolder, recursive = TRUE)
  }
  
  tableNames <- unique(tableNames)
  tableNames <- gsub(".csv$", "", tableNames)
  tableNames <- SqlRender::snakeCaseToCamelCase(tableNames)
  save(list = tableNames, file = file.path(dataFolder, "PreMerged.RData"), compress = TRUE)
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
  on.exit(rm(shinySettings, envir = .GlobalEnv))
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
