# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
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

#' Generate a set of cohorts
#'
#' @description
#' This function generates a set of cohorts in the cohort table.
#'
#' @param connection db connection
#' @template CdmDatabaseSchema
#' @template TempEmulationSchema
#' @param cohortDatabaseSchema        Cohort db schema name
#' @param cohortTableNames            Cohort table names
#' @param cohortDefinitionSet         Cohort definition set
#' @param stopOnError                 If an error happens while generating one of the cohorts in the
#'                                    cohortDefinitionSet, should we stop processing the other
#'                                    cohorts? The default is TRUE; when set to FALSE, failures will
#'                                    be identified in the return value from this function.
#'
#' @param incremental                 Create only cohorts that haven't been created before?
#'
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are
#'                                    kept of which definition has been executed.
#' @returns
#'
#' A data.frame consisting of the following columns:
#' \describe{
#'    \item{cohortId}{The unique integer identifier of the cohort}
#'    \item{cohortName}{The cohort's name}
#'    \item{generationStatus}{The status of the generation task which may be one of the following:
#'           \describe{
#'                \item{COMPLETE}{The generation completed successfully}
#'                \item{FAILED}{The generation failed (see logs for details)}
#'                \item{SKIPPED}{If using incremental == 'TRUE', this status indicates
#'                               that the cohort's generation was skipped since it
#'                               was previously completed.}
#'           }}
#'    \item{startTime}{The start time of the cohort generation. If the generationStatus == 'SKIPPED', the startTime will be NA.}
#'    \item{endTime}{The end time of the cohort generation. If the generationStatus == 'FAILED', the endTime will be the time of the failure.
#'                   If the generationStatus == 'SKIPPED', endTime will be NA.}
#'    }
#'
#' @export
generateCohortSet <- function(connection = NULL,
                              cdmDatabaseSchema,
                              tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                              cohortDatabaseSchema = cdmDatabaseSchema,
                              cohortTableNames = getCohortTableNames(),
                              cohortDefinitionSet = NULL,
                              stopOnError = TRUE,
                              incremental = FALSE,
                              incrementalFolder = NULL) {
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "sql"
                         )
  )
  if (is.null(connection)) {
    stop("You must provide either a database connection.")
  }
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }
  
  start <- Sys.time()
  
  # Verify the cohort tables exist and if they do not
  # stop the generation process
  tableExistsFlagList <- lapply(cohortTableNames, FUN = function(x) {
    x <- FALSE
  })
  tables <- getTableNames(connection, cohortDatabaseSchema)
  for (i in 1:length(cohortTableNames)) {
    if (toupper(cohortTableNames[i]) %in% toupper(tables)) {
      tableExistsFlagList[i] <- TRUE
    }
  }
  
  if (!all(unlist(tableExistsFlagList, use.names = FALSE))) {
    errorMsg <- "The following tables have not been created: \n"
    for (i in 1:length(cohortTableNames)) {
      if (!tableExistsFlagList[[i]]) {
        errorMsg <- paste0(errorMsg, "   - ", cohortTableNames[i], "\n")
      }
    }
    errorMsg <- paste(errorMsg, "Please use the createCohortTables function to ensure all tables exist before generating cohorts.", sep = "\n")
    stop(errorMsg)
  }
  
  
  if (incremental) {
    recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")
    
    if (isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))) {
      cohortDefinitionSet$checksum <- ""
      for (i in 1:nrow(cohortDefinitionSet)) {
        # This implementation supports recursive definitions (subsetting subsets) because the subsets have to be added in order
        if (cohortDefinitionSet$subsetParent[i] != cohortDefinitionSet$cohortId[i]) {
          j <- which(cohortDefinitionSet$cohortId == cohortDefinitionSet$subsetParent[i])
          cohortDefinitionSet$checksum[i] <- computeChecksum(paste(
            cohortDefinitionSet$sql[j],
            cohortDefinitionSet$sql[i]
          ))
        } else {
          cohortDefinitionSet$checksum[i] <- computeChecksum(cohortDefinitionSet$sql[i])
        }
      }
    } else {
      cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)
    }
  }
  # Create the cluster
  # DEV NOTE :: running subsets in a multiprocess setup will not work with subsets that subset other subsets
  # To resolve this issue we need to execute the dependency tree.
  # This could be done in an manner where all dependent cohorts are guaranteed to be sent to the same process and
  # the execution is in order. If you set numberOfThreads > 1 you should implement this!
  cluster <- ParallelLogger::makeCluster(numberOfThreads = 1)
  on.exit(ParallelLogger::stopCluster(cluster), add = TRUE)
  cohortsToGenerate <- cohortDefinitionSet$cohortId
  subsetsToGenerate <- c()
  # Generate top level cohorts first
  if (isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))) {
    cohortsToGenerate <- cohortDefinitionSet %>%
      dplyr::filter(!.data$isSubset) %>%
      dplyr::select("cohortId") %>%
      dplyr::pull()
    
    subsetsToGenerate <- cohortDefinitionSet %>%
      dplyr::filter(.data$isSubset) %>%
      dplyr::select("cohortId") %>%
      dplyr::pull()
  }
  
  # Apply the generation operation to the cluster
  cohortsGenerated <- ParallelLogger::clusterApply(
    cluster,
    cohortsToGenerate,
    generateCohort,
    cohortDefinitionSet = cohortDefinitionSet,
    connection = connection,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    stopIfError = stopOnError,
    incremental = incremental,
    recordKeepingFile = recordKeepingFile,
    stopOnError = stopOnError,
    progressBar = TRUE
  )
  subsetsGenerated <- c()
  if (length(subsetsToGenerate)) {
    subsetsGenerated <- ParallelLogger::clusterApply(
      cluster,
      subsetsToGenerate,
      generateCohort,
      cohortDefinitionSet = cohortDefinitionSet,
      connection = connection,
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      stopIfError = stopOnError,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      stopOnError = stopOnError,
      progressBar = TRUE
    )
  }
  
  # Convert the list to a data frame
  cohortsGenerated <- do.call(rbind, c(cohortsGenerated, subsetsGenerated))
  
  delta <- Sys.time() - start
  writeLines(paste("Generating cohort set took", round(delta, 2), attr(delta, "units")))
  invisible(cohortsGenerated)
}
