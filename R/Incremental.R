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

computeChecksum <- function(column) {
  return(sapply(as.character(column), digest::digest, algo = "md5", serialize = FALSE))
}

isTaskRequired <- function(..., checksum, recordKeepingFile, verbose = TRUE) {
  if (file.exists(recordKeepingFile)) {
    recordKeeping <-  readr::read_csv(recordKeepingFile, col_types = readr::cols())
    task <- recordKeeping[getKeyIndex(list(...), recordKeeping), ]
    if (nrow(task) == 0) {
      return(TRUE)
    }
    if (nrow(task) > 1) {
      stop("Duplicate key ", as.character(list(...)), " found in recordkeeping table")
    }
    if (task$checksum == checksum) {
      if (verbose) {
        key <- list(...)
        key <- paste(sprintf("%s = '%s'", names(key), key), collapse = ", ")
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

getKeyIndex <- function(key, recordKeeping) {
  if (nrow(recordKeeping) == 0 || length(key[[1]]) == 0 || !all(names(key) %in% names(recordKeeping))) {
    return(c())
  } else {
    key <- unique(tibble::as_tibble(key))
    recordKeeping$idxCol <- 1:nrow(recordKeeping)
    idx <- merge(recordKeeping, key)$idx
    return(idx)
  }
}

recordTasksDone <- function(..., checksum, recordKeepingFile) {
  if (file.exists(recordKeepingFile)) {
    recordKeeping <-  readr::read_csv(recordKeepingFile, col_types = readr::cols())
    idx <- getKeyIndex(list(...), recordKeeping)
    if (length(idx) > 0) {
      recordKeeping <- recordKeeping[-idx, ]
    }
  } else {
    recordKeeping <- tibble::tibble()
  }
  newRow <- tibble::as.tibble(list(...))
  newRow$checksum <- checksum
  newRow$timeStamp <-  Sys.time()
  recordKeeping <- dplyr::bind_rows(recordKeeping, newRow)
  readr::write_csv(recordKeeping, recordKeepingFile)
}

saveIncremental <- function(data, fileName, ...) {
  if (file.exists(fileName)) {
    previousData <- readr::read_csv(fileName, col_types = readr::cols())
    idx <- getKeyIndex(list(...), previousData)
    if (length(idx) > 0) {
      previousData <- previousData[-idx, ] 
    }
    data <- dplyr::bind_rows(previousData, data)
  } 
  readr::write_csv(data, fileName)
}
