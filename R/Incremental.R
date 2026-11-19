# Copyright 2025 Observational Health Data Sciences and Informatics
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

computeChecksum <- function(column) {
  return(sapply(
    as.character(column),
    digest::digest,
    algo = "md5",
    serialize = FALSE
  ))
}

#' Compute a run-level fingerprint of the settings that affect results
#'
#' Any change to these settings should invalidate previously computed results so
#' that repeat runs recompute rather than silently reusing stale output.
#'
#' @param context A DiagnosticsContext object.
#' @return A single md5 hash.
#' @noRd
computeRunFingerprint <- function(context) {
  settings <- list(
    callingArgsJson = if (is.null(context$callingArgsJson)) "{}" else context$callingArgsJson,
    cdmVersion = context$cdmVersion,
    databaseId = context$databaseId,
    packageVersion = tryCatch(
      as.character(utils::packageVersion("CohortDiagnostics")),
      error = function(e) ""
    )
  )
  return(digest::digest(settings, algo = "md5"))
}

#' Compute per-cohort checksums combining the cohort definition with a run fingerprint
#'
#' @param cohortDefinitionSet Data frame with at least cohortId. cohortName, sql and
#'   json are included when present.
#' @param runFingerprint A run-level fingerprint created by \code{computeRunFingerprint}.
#' @return A character vector of md5 hashes, one per cohort.
#' @noRd
computeCohortChecksums <- function(cohortDefinitionSet, runFingerprint = "") {
  if (nrow(cohortDefinitionSet) == 0) {
    return(character(0))
  }
  cohortName <- if ("cohortName" %in% names(cohortDefinitionSet)) as.character(cohortDefinitionSet$cohortName) else ""
  sql <- if ("sql" %in% names(cohortDefinitionSet)) as.character(cohortDefinitionSet$sql) else ""
  json <- if ("json" %in% names(cohortDefinitionSet)) as.character(cohortDefinitionSet$json) else ""
  column <- paste(
    as.character(cohortDefinitionSet$cohortId),
    cohortName,
    sql,
    json,
    as.character(runFingerprint),
    sep = "\r"
  )
  return(computeChecksum(column))
}

#' Resolve the merge/removal key columns for a result file from the results data model
#'
#' @param fileName Path to the result csv file. The table name is derived from the
#'   file name (without the .csv extension).
#' @return A character vector of primary key column names (may be empty).
#' @noRd
getTablePrimaryKey <- function(fileName) {
  tableName <- sub("\\.csv$", "", basename(fileName))
  spec <- getResultsDataModelSpecifications()
  return(spec$columnName[spec$tableName == tableName & spec$primaryKey == "Yes"])
}

#' Determine whether a result table must always be written in full (never incrementally)
#'
#' @param fileName Path to the result csv file.
#' @return TRUE if the table is flagged as \code{never_incremental}.
#' @noRd
isNeverIncremental <- function(fileName) {
  tableName <- sub("\\.csv$", "", basename(fileName))
  spec <- getResultsDataModelSpecifications()
  flags <- spec$neverIncremental[spec$tableName == tableName]
  return(length(flags) > 0 && any(flags == "Yes"))
}

isTaskRequired <-
  function(...,
           checksum,
           recordKeepingFile,
           verbose = TRUE) {
    if (file.exists(recordKeepingFile)) {
      readr::local_edition(1)
      recordKeeping <- readr::read_csv(recordKeepingFile,
        col_types = readr::cols(),
        guess_max = min(1e7),
        lazy = FALSE
      )
      task <- recordKeeping[getKeyIndex(list(...), recordKeeping), ]
      if (nrow(task) == 0) {
        return(TRUE)
      }
      if (nrow(task) > 1) {
        stop(
          "Duplicate key ",
          as.character(list(...)),
          " found in recordkeeping table"
        )
      }
      if (task$checksum == checksum) {
        if (verbose) {
          key <- list(...)
          key <-
            paste(sprintf("%s = '%s'", names(key), key), collapse = ", ")
          ParallelLogger::logInfo("Skipping ", key, " because unchanged from earlier run")
        }
        return(FALSE)
      } else {
        return(TRUE)
      }
    } else {
      return(TRUE)
    }
  }

getRequiredTasks <- function(..., checksum, recordKeepingFile) {
  tasks <- list(...)
  if (length(tasks) == 0 || length(tasks[[1]]) == 0) {
    return(dplyr::tibble())
  }
  tasks <- dplyr::as_tibble(tasks)
  tasks$checksum <- checksum

  if (file.exists(recordKeepingFile)) {
    readr::local_edition(1)
    recordKeeping <- readr::read_csv(recordKeepingFile,
      col_types = readr::cols(),
      guess_max = min(1e7),
      lazy = FALSE
    )

    if (all(names(tasks) %in% names(recordKeeping))) {
      # getKeyIndex returns row indices into its second argument, so pass tasks
      # second to obtain the indices of already-completed tasks to remove.
      idx <- getKeyIndex(recordKeeping[, names(tasks)], tasks)
    } else {
      idx <- integer(0)
    }

    if (length(idx) > 0) {
      tasks <- tasks[-idx, ]
    }
  }
  tasks$checksum <- NULL
  return(tasks)
}

#' Find the indices of rows in \code{recordKeeping} that match \code{key}
#'
#' @param key A list/tibble of key values (one or more columns).
#' @param recordKeeping A tibble of existing records.
#' @return Integer vector of row indices into \code{recordKeeping}. Returns
#'   \code{integer(0)} when there are no matches or the key is incompatible.
#' @noRd
getKeyIndex <- function(key, recordKeeping) {
  if (nrow(recordKeeping) == 0 ||
    length(key[[1]]) == 0 ||
    !all(names(key) %in% names(recordKeeping))) {
    return(integer(0))
  }
  key <- dplyr::as_tibble(key) %>% dplyr::distinct()
  recordKeeping$idxCol <- seq_len(nrow(recordKeeping))
  byColumns <- intersect(names(recordKeeping), names(key))
  idx <- merge(recordKeeping, key, by = byColumns)$idxCol
  return(as.integer(idx))
}

recordTasksDone <-
  function(...,
           checksum,
           recordKeepingFile,
           incremental = TRUE) {
    if (!incremental) {
      return()
    }
    if (length(list(...)[[1]]) == 0) {
      return()
    }

    if (file.exists(recordKeepingFile)) {
      readr::local_edition(1)
      # reading record keeping file into memory
      # prevent lazy loading to avoid lock on file
      recordKeeping <- readr::read_csv(
        file = recordKeepingFile,
        col_types = readr::cols(),
        guess_max = min(1e7),
        lazy = FALSE
      )

      recordKeeping$timeStamp <-
        as.character(recordKeeping$timeStamp)
      if ("cohortId" %in% colnames(recordKeeping)) {
        recordKeeping <- recordKeeping %>%
          dplyr::mutate(cohortId = as.double(.data$cohortId))
      }
      if ("comparatorId" %in% colnames(recordKeeping)) {
        recordKeeping <- recordKeeping %>%
          dplyr::mutate(comparatorId = as.double(.data$comparatorId))
      }
      idx <- getKeyIndex(list(...), recordKeeping)
      if (length(idx) > 0) {
        recordKeeping <- recordKeeping[-idx, ]
      }
    } else {
      recordKeeping <- dplyr::tibble()
    }
    newRow <- dplyr::as_tibble(list(...))
    newRow$checksum <- checksum
    newRow$timeStamp <- as.character(Sys.time())
    recordKeeping <- dplyr::bind_rows(recordKeeping, newRow)
    writeCsvAtomic(data = recordKeeping, fileName = recordKeepingFile)
  }

#' Write a data frame to csv atomically (write to temp file, then rename)
#'
#' @param data The data frame to write.
#' @param fileName The destination file.
#' @return Invisibly NULL.
#' @noRd
writeCsvAtomic <- function(data, fileName) {
  readr::local_edition(1)
  tempName <- paste0(fileName, ".tmp")
  readr::write_csv(x = data, file = tempName, na = "")
  if (file.exists(fileName)) {
    unlink(fileName)
  }
  file.rename(from = tempName, to = fileName)
  return(invisible(NULL))
}

# Normalize columns that are prone to type drift between runs (e.g. numeric ids
# read back as doubles, empty columns read back as logical).
normalizeIncrementalColumns <- function(data) {
  if ("database_id" %in% colnames(data)) {
    data$database_id <- as.character(data$database_id)
  }
  return(data)
}

#' @noRd
writeToCsv <- function(data, fileName, incremental = FALSE, ...) {
  UseMethod("writeToCsv", data)
}

#' @exportS3Method writeToCsv tbl_Andromeda
#' @noRd
writeToCsv.tbl_Andromeda <- function(data, fileName, incremental = FALSE, ...) {
  writeToCsv.default(
    data = dplyr::collect(data),
    fileName = fileName,
    incremental = incremental,
    ...
  )
}

#' @noRd
writeToCsv.default <- function(data, fileName, incremental = FALSE, ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  if (incremental) {
    params <- list(...)
    if (length(params) > 0) {
      names(params) <- SqlRender::camelCaseToSnakeCase(names(params))
    }
    params$data <- data
    params$fileName <- fileName
    do.call(saveIncremental, params)
    ParallelLogger::logDebug("appending records to ", fileName)
  } else {
    if (file.exists(fileName)) {
      ParallelLogger::logDebug(
        "Overwriting and replacing previous ",
        fileName,
        " with new."
      )
    } else {
      ParallelLogger::logDebug("creating ", fileName)
    }
    readr::local_edition(1)
    readr::write_excel_csv(
      x = data,
      file = fileName,
      na = "",
      append = FALSE,
      delim = ","
    )
  }
}

#' Incrementally save a data frame to csv, replacing existing rows that share the
#' merge key of the incoming data.
#'
#' The merge key is derived from the (snake_case) named arguments in \code{...}.
#' If no key is supplied, the primary key declared in the results data model for
#' the table matching \code{fileName} is used as a fallback.
#'
#' @param data The data frame to write.
#' @param fileName The destination csv file.
#' @param ... Named vectors describing the merge key (column name = values).
#' @return Invisibly NULL.
#' @noRd
saveIncremental <- function(data, fileName, ...) {
  # Tables flagged never_incremental are always written in full.
  if (isNeverIncremental(fileName)) {
    return(writeCsvAtomic(data = data, fileName = fileName))
  }

  key <- list(...)
  if (length(key) > 0 && length(key[[1]]) == 0) {
    # Empty key (e.g. no cohorts to process) - nothing to write.
    return(invisible(NULL))
  }

  primaryKey <- intersect(getTablePrimaryKey(fileName), colnames(data))

  # Fallback: derive the merge key from the results data model primary key.
  if (length(key) == 0 && length(primaryKey) > 0) {
    key <- lapply(primaryKey, function(column) unique(data[[column]]))
    names(key) <- primaryKey
  }

  if (file.exists(fileName) && file.size(fileName) > 0) {
    readr::local_edition(1)
    previousData <- readr::read_csv(fileName,
      col_types = readr::cols(),
      guess_max = min(1e7),
      lazy = FALSE
    )
    if ((nrow(previousData)) > 0) {
      previousData <- normalizeIncrementalColumns(previousData)
      data <- normalizeIncrementalColumns(data)

      if (length(key) > 0 && all(names(key) %in% names(previousData))) {
        idx <- getKeyIndex(key, previousData)
        if (length(idx) > 0) {
          previousData <- previousData[-idx, ]
        }
      }
      if (nrow(previousData) > 0) {
        data <- dplyr::bind_rows(data, previousData) %>%
          dplyr::distinct() %>%
          tidyr::tibble()
      } else {
        data <- data %>% tidyr::tibble()
      }
    }
  }

  # Safety net: enforce primary key uniqueness in the merged output. This guards
  # against duplicate result rows (e.g. duplicated cohort overlap covariate
  # values) even if a caller supplies a merge key narrower than the primary key.
  # Incoming rows take precedence because they were bound first.
  if (length(primaryKey) > 0) {
    data <- data %>%
      dplyr::distinct(dplyr::across(dplyr::all_of(primaryKey)), .keep_all = TRUE)
  }

  writeCsvAtomic(data = data, fileName = fileName)
  return(invisible(NULL))
}

subsetToRequiredCohorts <-
  function(cohorts,
           task,
           incremental,
           recordKeepingFile) {
    if (incremental) {
      tasks <- getRequiredTasks(
        cohortId = cohorts$cohortId,
        task = task,
        checksum = cohorts$checksum,
        recordKeepingFile = recordKeepingFile
      )
      return(cohorts[cohorts$cohortId %in% tasks$cohortId, ])
    } else {
      return(cohorts)
    }
  }

#' Remove record-keeping entries for cohorts that are no longer part of the run
#'
#' Only safe to call for a full (non-subset) run; the caller is responsible for
#' ensuring the supplied \code{cohortIds} represents the complete set of cohorts
#' that should be retained.
#'
#' @param recordKeepingFile The path to the record-keeping ledger.
#' @param cohortIds The cohort ids that should be retained.
#' @return Invisibly NULL.
#' @noRd
pruneRecordKeeping <- function(recordKeepingFile, cohortIds) {
  if (!file.exists(recordKeepingFile) || file.size(recordKeepingFile) == 0) {
    return(invisible(NULL))
  }
  readr::local_edition(1)
  recordKeeping <- readr::read_csv(
    file = recordKeepingFile,
    col_types = readr::cols(),
    guess_max = min(1e7),
    lazy = FALSE
  )
  if (nrow(recordKeeping) == 0 || !"cohortId" %in% colnames(recordKeeping)) {
    return(invisible(NULL))
  }
  recordKeeping$cohortId <- as.double(recordKeeping$cohortId)
  recordKeeping <- recordKeeping[recordKeeping$cohortId %in% as.double(cohortIds), , drop = FALSE]
  writeCsvAtomic(data = recordKeeping, fileName = recordKeepingFile)
  return(invisible(NULL))
}
