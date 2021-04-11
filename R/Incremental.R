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

computeChecksum <- function(column) {
  return(sapply(as.character(column), digest::digest, algo = "md5", serialize = FALSE))
}

isTaskRequired <- function(..., checksum, recordKeepingFile, verbose = TRUE) {
  if (file.exists(recordKeepingFile)) {
    recordKeeping <-  readr::read_csv(recordKeepingFile, 
                                      col_types = readr::cols(),
                                      guess_max = min(1e7))
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

getRequiredTasks <- function(..., checksum, recordKeepingFile) {
  tasks <- list(...)
  if (file.exists(recordKeepingFile) && length(tasks[[1]]) > 0) {
    recordKeeping <-  readr::read_csv(recordKeepingFile, 
                                      col_types = readr::cols(), 
                                      guess_max = min(1e7))
    tasks$checksum <- checksum
    tasks <- dplyr::as_tibble(tasks)
    if (all(names(tasks) %in% names(recordKeeping))) {
      idx <- getKeyIndex(recordKeeping[, names(tasks)], tasks)
    } else {
      idx = c()
    }
    tasks$checksum <- NULL
    if (length(idx) > 0) {
      # text <- paste(sprintf("%s = %s", names(tasks), tasks[idx,]), collapse = ", ")
      # ParallelLogger::logInfo("Skipping ", text, " because unchanged from earlier run")
      tasks <- tasks[-idx, ]
    }
  }
  return(tasks)
}

getKeyIndex <- function(key, recordKeeping) {
  if (nrow(recordKeeping) == 0 || length(key[[1]]) == 0 || !all(names(key) %in% names(recordKeeping))) {
    return(c())
  } else {
    key <- dplyr::as_tibble(key) %>% dplyr::distinct()
    recordKeeping$idxCol <- 1:nrow(recordKeeping)
    idx <- merge(recordKeeping, key)$idx
    return(idx)
  }
}

recordTasksDone <- function(..., checksum, recordKeepingFile, incremental = TRUE) {
  if (!incremental) {
    return()
  }
  if (length(list(...)[[1]]) == 0) {
    return()
  }
  if (file.exists(recordKeepingFile)) {
    recordKeeping <-  readr::read_csv(recordKeepingFile, 
                                      col_types = readr::cols(),
                                      guess_max = min(1e7))
    recordKeeping$timeStamp <- as.character(recordKeeping$timeStamp)
    if ('cohortId' %in% colnames(recordKeeping)) {
      recordKeeping <- recordKeeping %>% 
        dplyr::mutate(cohortId = as.double(.data$cohortId))
    }
    if ('comparatorId' %in% colnames(recordKeeping)) {
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
  newRow$timeStamp <-  as.character(Sys.time())
  recordKeeping <- dplyr::bind_rows(recordKeeping, newRow)
  readr::write_csv(recordKeeping, recordKeepingFile)
}

writeToCsv <- function(data, fileName, incremental = FALSE, ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  if (incremental) {
    params <- list(...)
    names(params) <- SqlRender::camelCaseToSnakeCase(names(params))
    params$data = data
    params$fileName = fileName
    do.call(saveIncremental, params)
    ParallelLogger::logDebug("appending records to ", fileName)
  } else {
    if (file.exists(fileName)) {
      ParallelLogger::logDebug("Overwriting and replacing previous ",fileName, " with new.")
    } else {
      ParallelLogger::logDebug("creating ",fileName)
    }
    readr::write_excel_csv(x = data, 
                           file = fileName, 
                           na = "", 
                           append = FALSE,
                           delim = ",")
  }
}

writeCovariateDataAndromedaToCsv <- function(data, fileName, incremental = FALSE) {
  if (incremental && file.exists(fileName)) {
    ParallelLogger::logDebug("Appending records to ", fileName)
    batchSize <- 1e5
    
    cohortIds <- data %>%
      distinct(.data$cohortId) %>%
      pull()
    
    tempName <- paste0(fileName, "2")
    
    processChunk <- function(chunk, pos) {
      chunk <- chunk %>%
        filter(!.data$cohort_id %in% cohortIds)
      readr::write_csv(chunk, tempName, append = (pos != 1))
    }
    
    readr::read_csv_chunked(file = fileName, 
                            callback = processChunk,
                            chunk_size = batchSize,
                            col_types = readr::cols(),
                            guess_max = batchSize)
    
    addChunk <- function(chunk) {
      colnames(chunk) <- SqlRender::camelCaseToSnakeCase(colnames(chunk))
      readr::write_csv(chunk, tempName, append = TRUE)
    }
    Andromeda::batchApply(data, addChunk)
    unlink(fileName)                       
    file.rename(tempName, fileName)  
  } else {
    if (file.exists(fileName)) {
      ParallelLogger::logDebug("Overwriting and replacing previous ",fileName, " with new.")
      unlink(fileName)
    } else {
      ParallelLogger::logDebug("Creating ",fileName)
    }
    writeToFile <- function(batch) {
      first <- !file.exists(fileName)
      if (first) {
        colnames(batch) <- SqlRender::camelCaseToSnakeCase(colnames(batch))
      }
      readr::write_csv(batch, fileName, append = !first)
    }
    Andromeda::batchApply(data, writeToFile)
  }
}

saveIncremental <- function(data, fileName, ...) {
  if (!length(list(...)) == 0) {
    if (length(list(...)[[1]]) == 0) {
      return()
    }
  }
  if (file.exists(fileName)) {
    previousData <- readr::read_csv(fileName, 
                                    col_types = readr::cols(),
                                    guess_max = min(1e7))
    if ((nrow(previousData)) > 0) {
      if (!length(list(...)) == 0) {
        idx <- getKeyIndex(list(...), previousData)
      } else {
        idx <- NULL
      }
      if (length(idx) > 0) {
        previousData <- previousData[-idx, ] 
      }
      if (nrow(previousData) > 0) {
        data <- dplyr::bind_rows(previousData, data) %>% 
          dplyr::distinct() %>% 
          tidyr::tibble()
      } else {
        data <- data %>% tidyr::tibble()
      }
      
    }
  } 
  readr::write_csv(data, fileName)
}

subsetToRequiredCohorts <- function(cohorts, task, incremental, recordKeepingFile) {
  if (incremental) {
    tasks <- getRequiredTasks(cohortId = cohorts$cohortId,
                              task = task,
                              checksum = cohorts$checksum,
                              recordKeepingFile = recordKeepingFile)
    return(cohorts[cohorts$cohortId %in% tasks$cohortId, ])
  } else {
    return(cohorts)
  }
}

subsetToRequiredCombis <- function(combis, task, incremental, recordKeepingFile) {
  if (incremental) {
    tasks <- getRequiredTasks(cohortId = combis$targetCohortId,
                              comparatorId = combis$comparatorCohortId,
                              task = task,
                              checksum = combis$checksum,
                              recordKeepingFile = recordKeepingFile)
    return(merge(combis, dplyr::tibble(targetCohortId = tasks$cohortId, comparatorCohortId = tasks$comparatorId)))
  } else {
    return(combis)
  }
}
