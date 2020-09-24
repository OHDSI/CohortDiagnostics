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
# 

#' Create a DDL script for results data model from specification csv. 
#'
#' @param specification  The location of the csv file with the high-level results table specification.
#' @param packageVersion The version number of cohort diagnostics
#' @param modelVersion   The version of the results data model
#' @param packageName    The name of the R package whose output model we are documenting.
#' 
#' @export
createDdl <- function(packageName,
                      packageVersion,
                      modelVersion,
                      specification){
  
  tableList <- specification$tableName %>% unique()
  
  script <- c()
  script <- c(script, paste0("--DDL Specification for package ", packageName, " package version: ", packageVersion, '\n'))
  script <- c(script, paste0("--Data Model Version ", modelVersion, '\n'))
  script <- c(script, paste0("--Last update ", Sys.Date(), '\n'))
  script <- c(script, paste0("--Number of tables ", length(tableList), '\n'))
  
  for (i in (1:length(tableList))) {
    script <- c(script, paste0('\n'))
    script <- c(script, paste0('-----------------------------------------------------------------------'))
    script <- c(script, paste0('\n'))
    script <- c(script, paste0("--Table name ", tableList[[i]], '\n'))
    table <- specification %>% 
      dplyr::filter(.data$tableName == tableList[[i]])
    
    fields <- table %>% dplyr::select(.data$fieldName) %>% dplyr::pull()
    script <- c(script, paste0("--Number of fields in table ", length(fields), '\n'))
    hint <- "--HINT DISTRIBUTE ON RANDOM\n"
    script <- c(script, hint, paste0("CREATE TABLE @resultsDatabaseSchema.", tableList[[i]], " (\n"))
    end <- length(fields)
    
    a <- c()
    for (f in fields) { #from https://github.com/OHDSI/CdmDdlBase/blob/f256bd2a3350762e4a37108986711516dd5cd5dc/R/createDdlFromFile.R#L50
      if (subset(table, .data$fieldName == f, .data$isRequired) == "Yes") {
        r <- (" NOT NULL")
      } else {
        r <- (" NULL")
      }
      if (f == fields[[end]]) {
        e <- (" );")
      } else {
        e <- (",")
      }
      a <- c(a, paste0("\n\t\t\t",f," ",subset(table, .data$fieldName == f, .data$type), r, e))
    }
    script <- c(script, a, "")
    script <- c(script, paste0('\n'))
  }
  return(script)
}



#' Create DDL with primary key
#'
#' @param specification  The location of the csv file with the high-level results table specification.
#' @param packageVersion The version number of cohort diagnostics
#' @param modelVersion   The version of the results data model
#' @param packageName    The name of the R package whose output model we are documenting.
#' 
#' @export
#' 
createDdlPkConstraints <- function(packageName,
                                   packageVersion,
                                   modelVersion,
                                   specification){
  
  script <- c()
  script <- c(script, paste0("--DDL Primary Key Constraints Specification for package ", 
                             packageName, " package version: ", packageVersion, '\n'))
  script <- c(script, paste0("--Data Model Version ", modelVersion, '\n'))
  script <- c(script, paste0("--Last update ", Sys.Date(), '\n'))
  
  tableList <- specification$tableName %>% unique()
  script <- c(script, paste0("--Number of tables ", length(tableList), '\n'))
  
  for (i in (1:length(tableList))) {
    table <- specification %>% 
      dplyr::filter(.data$tableName == tableList[[i]]) %>% 
      dplyr::filter(.data$primaryKey == 'Yes')
    
    if (nrow(table) > 0) {
      primaryKey <- paste0(table$fieldName, collapse = ",")
      pk <- paste0("ALTER TABLE @resultsDatabaseSchema.",
                   tableList[[i]],
                   " ADD CONSTRAINT xpk_",
                   tableList[[i]],
                   " PRIMARY KEY NONCLUSTERED (",
                   primaryKey,
                   ");")
      script <- c(script, paste0('\n'))
      script <- c(script, pk, "")
    }
  }
  return(script)
}



#' Create DDL that drops all results table
#'
#' @param specification  The location of the csv file with the high-level results table specification.
#' @param packageVersion The version number of cohort diagnostics
#' @param modelVersion   The version of the results data model
#' @param packageName    The name of the R package whose output model we are documenting.
#' 
#' @export
#' 
dropDdl <- function(packageName,
                    packageVersion,
                    modelVersion,
                    specification){
  
  script <- c()
  script <- c(script, paste0("--DDL Drop table Specification for package ", 
                             packageName, " package version: ", packageVersion, '\n'))
  script <- c(script, paste0("--Data Model Version ", modelVersion, '\n'))
  script <- c(script, paste0("--Last update ", Sys.Date(), '\n'))
  
  tableList <- specification$tableName %>% unique()
  script <- c(script, paste0("--Number of tables ", length(tableList), '\n'))
  
  for (i in (1:length(tableList))) {
    table <- specification %>% 
      dplyr::filter(.data$tableName == tableList[[i]]) 
    
    if (nrow(table) > 0) {
      pk <- paste0("DROP TABLE IF EXISTS @resultsDatabaseSchema.",
                   tableList[[i]],
                   ";")
      script <- c(script, paste0('\n'))
      script <- c(script, pk, "")
    }
  }
  return(script)
}



#' Guesses data model specification from multiple csv files.
#'
#' @param pathToCsvFile file system path to csv file
#' 
#' @return 
#' A tibble data frame object with specifications
#' 
#' @examples
#' \dontrun{
#' csvFileSpecification <- guessCsvFileSpecification(path)
#' }
#' 
#' @export
#' 
guessCsvFileSpecification <- function(pathToCsvFile) {
  tableToWorkOn <-  stringr::str_remove(string = basename(pathToCsvFile), 
                                        pattern = ".csv")
  
  print(paste0("Reading csv files '", tableToWorkOn, "' and guessing data types."))
  
  csvFile <- readr::read_csv(file = pathToCsvFile,
                             col_types = readr::cols(),
                             guess_max = min(1e7),
                             locale = readr::locale(encoding = "UTF-8"))
  if (any(stringr::str_detect(string = colnames(csvFile), pattern = "_"))) {
    colnames(csvFile) <- tolower(colnames(csvFile))
  }
  
  patternThatIsNotPrimaryKey = c("subjects", "entries", "name", "sql", "json", "description", "atlas_id", "day")
  patternThatIsPrimaryKey = c('_id', 'rule_sequence')
  describe <- list()
  primaryKeyIfOmopVocabularyTable <-  getPrimaryKeyForOmopVocabularyTable() %>% 
    dplyr::filter(.data$vocabularyTableName == tableToWorkOn %>% tolower()) %>% 
    dplyr::pull(.data$primaryKey) %>% 
    strsplit(split = ",") %>% 
    unlist() %>% 
    tolower()
  
  for (i in (1:length(colnames(csvFile)))) {
    tableName <- tableToWorkOn
    fieldName <- colnames(csvFile)[[i]]
    fieldData <- csvFile %>% dplyr::select(fieldName)
    dataVector <- fieldData %>% dplyr::pull(1)
    type <- suppressWarnings(guessDbmsDataTypeFromVector(value = dataVector))
    if (stringr::str_detect(string = fieldName, 
                            pattern = stringr::fixed('_id')) &&
        type == 'float') {
      type = 'bigint'
    }
    if (stringr::str_detect(string = tolower(fieldName), 
                            pattern = stringr::fixed('description')) &&
        (stringr::str_detect(string = type, 
                             pattern = 'varchar') ||
         stringr::str_detect(string = type, 
                             pattern = 'logical')
        )
    ) {
      type = 'varchar(max)'
    }
    isRequired <- 'Yes'
    if (anyNA(csvFile %>% dplyr::pull(fieldName))) {
      isRequired <- 'No'
    }
    primaryKey <- 'No'
    if (tableName %in% getPrimaryKeyForOmopVocabularyTable()$vocabularyTableName && 
        fieldName %in% primaryKeyIfOmopVocabularyTable) {
      primaryKey <- 'Yes'
    } else if (isRequired == 'Yes' &&
               nrow(csvFile) == nrow(csvFile %>% dplyr::select(fieldName) %>% dplyr::distinct()) &&
               all(stringr::str_detect(string = fieldName, 
                                       pattern = patternThatIsNotPrimaryKey, 
                                       negate = TRUE))) {
      primaryKey <- 'Yes'
    } else if (isRequired == 'Yes' &&
               any(stringr::str_detect(string = fieldName, 
                                       pattern = patternThatIsPrimaryKey))) {
      primaryKey <- 'Yes'
    }
    describe[[i]] <- tidyr::tibble(tableName = tableName, 
                                   fieldName = fieldName,
                                   type = type,
                                   isRequired = isRequired,
                                   primaryKey = primaryKey)
    
    if (describe[[i]]$type == 'logical') {
      describe[[i]]$type == 'varchar(1)'
    }
    if (describe[[i]]$tableName == 'cohort' && 
        describe[[i]]$fieldName == 'cohort_name' &&
        describe[[i]]$type == 'float') {
      describe[[i]]$type = 'varchar(255)'
    }
    if (describe[[i]]$tableName == 'incidence_rate' && describe[[i]]$fieldName == 'calendar_year') {
      describe[[i]]$primaryKey = 'Yes'
    }
    if (describe[[i]]$tableName %in% c('covariate_value', 'temporal_covariate_value', 'time_distribution') && 
        describe[[i]]$fieldName %in% c('covariate_id','start_day','end_day')) {
      describe[[i]]$primaryKey = 'Yes'
    }
    if (describe[[i]]$tableName %in% c('included_source_concept','index_event_breakdown', 'orphan_concept')) {
      if (describe[[i]]$fieldName %in% c('concept_set_id', 'concept_id', 'source_concept_id')) {
        describe[[i]]$primaryKey = 'Yes'
      }
    }
  }
  describe <- dplyr::bind_rows(describe)
  return(describe)
}


getPrimaryKeyForOmopVocabularyTable <- function() {
  vocabularyTableKeys <- dplyr::bind_rows(
    tidyr::tibble(vocabularyTableName = 'concept', primaryKey = 'concept_id'),
    tidyr::tibble(vocabularyTableName = 'vocabulary', primaryKey = 'vocabulary_id'),
    tidyr::tibble(vocabularyTableName = 'domain', primaryKey = 'domain_id'),
    tidyr::tibble(vocabularyTableName = 'concept_class', primaryKey = 'concept_class_id'),
    tidyr::tibble(vocabularyTableName = 'concept_relationship', primaryKey = 'concept_id_1,concept_id_2,relationship_id'),
    tidyr::tibble(vocabularyTableName = 'relationship', primaryKey = 'relationship_id'),
    tidyr::tibble(vocabularyTableName = 'concept_ancestor', primaryKey = 'ancestor_concept_id,descendant_concept_id'),
    tidyr::tibble(vocabularyTableName = 'source_to_concept_map', primaryKey = 'source_vocabulary_id,target_concept_id,source_code,valid_end_date'),
    tidyr::tibble(vocabularyTableName = 'drug_strength', primaryKey = 'drug_concept_id, ingredient_concept_id'))
  
  return(vocabularyTableKeys)
}


#' Get specification for Cohort Diagnostics results data model
#' 
#' @return 
#' A tibble data frame object with specifications
#' 
#' @examples
#' \dontrun{
#' resultsDataModelSpecification <- getResultsDataModelSpecification()
#' }
#' 
#' @export
getResultsDataModelSpecification <- function() {
  pathToCsvFile <- system.file('sql',
                               'resultsDataModel',
                               'specification.csv', 
                               package = 'CohortDiagnostics')
  specification <- readr::read_csv(file = pathToCsvFile,
                                   col_types = readr::cols(),
                                   guess_max = min(1e7),
                                   locale = readr::locale(encoding = "UTF-8"))
  return(specification)
}


guessDbmsDataTypeFromVector <- function(value) {
  class <- value %>% class() %>% max()
  type <- value %>% typeof() %>% max()
  mode <- value %>% mode() %>% max()
  if (type == 'double' && class == 'Date' && mode == 'numeric') {
    type = 'Date'
  } else if (type == 'double' && (any(class %in% c("POSIXct", "POSIXt")))  && mode == 'numeric') {
    type = 'DATETIME2'
  } else if (type == 'double' && class == 'numeric' && mode == 'numeric') { #in R double and numeric are same
    type = 'float'
  } else if (class == 'integer' && type == 'integer' && mode == 'integer') {
    type = 'integer'
  } else if (type == 'character' && class == 'character' && mode == 'character') {
    fieldCharLength <- try(max(stringr::str_length(value)) %>% 
                             as.integer())
    if (is.na(fieldCharLength)) {
      fieldCharLength = 9999
    }
    if (fieldCharLength <= 1) {
      fieldChar = '1'
    } else if (fieldCharLength <= 20) {
      fieldChar = '20'
    } else if (fieldCharLength <= 50) {
      fieldChar = '50'
    } else if (fieldCharLength <= 255) {
      fieldChar = '255'
    } else {
      fieldChar = 'max'
    }
    type = paste0('varchar(', fieldChar, ')')
  } else if (class == "logical") {
    type <- 'varchar(1)'
  } else {
    type <- 'Unknown'
  }
  return(type)
}



#' Combine CSVs that are part of Cohort Diagnostics results data model
#' 
#' @return 
#' This function searches for csv files that conforms to the Cohort 
#' Diagnostics results data model, parses them, does quality checks,
#' appends files and makes them ready for upload into a dbms. Note:
#' the files are expected to be in snake_case format.
#' 
#' @param inputLocation   Input location for files
#' @param outputLocation  Output location for files
#' @param recursive       Search for files recursively
#' @param reportLocation  (Optional) Output data quality report to this location.
#' 
#' @return 
#' NULL
#' 
#' @examples
#' \dontrun{
#' combineResultsDataModelCsvFiles()
#' }
#' 
#' @export
combineResultsDataModelCsvFiles <- function(inputLocation,
                                            outputLocation = file.path(inputLocation, 'combined'),
                                            recursive = TRUE,
                                            # lookForCsvWithinZipFiles = TRUE, TO DO
                                            reportLocation = NULL) {
  unlink(x = outputLocation, recursive = TRUE, force = TRUE)
  dir.create(path = outputLocation, showWarnings = FALSE, recursive = TRUE)
  
  if (!is.null(reportLocation)) {
    reportLocation <- NULL
    ParallelLogger::logInfo("TO DO - report generation.")
  }
  
  resultsDataModelSpecification <- 
    readr::read_csv(file = system.file('settings',
                                       'resultsDataModelSpecification.csv',
                                       package = 'CohortDiagnostics'),
                    col_types = readr::cols(), 
                    guess_max = min(1e7), 
                    locale = readr::locale(encoding = "UTF-8"))
  
  ParallelLogger::logInfo("  Searching for the following files:",
                          paste0("\n    - ", resultsDataModelSpecification$tableName %>% unique() %>% sort(), ".csv"))
  
  csvFiles <- tidyr::tibble(fullName = list.files(path = inputLocation, 
                                                  pattern = ".csv", 
                                                  recursive = recursive, 
                                                  full.names = TRUE, 
                                                  ignore.case = FALSE)) %>% 
    dplyr::mutate(tableName = stringr::str_replace(string = basename(.data$fullName),
                                                   pattern = ".csv",
                                                   replacement = "")) %>% 
    dplyr::inner_join(resultsDataModelSpecification %>% 
                        dplyr::select(.data$tableName) %>% 
                        dplyr::distinct()
    ) %>% 
    dplyr::filter(stringr::str_detect(string = basename(.data$fullName), 
                                      pattern = basename(outputLocation), 
                                      negate = TRUE))
  
  message1 <- paste0("  Found the following csv files:",
                     (csvFiles %>% 
                        dplyr::group_by(.data$tableName) %>% 
                        dplyr::summarise(count = dplyr::n()) %>% 
                        dplyr::mutate(report = paste0("\n     - " ,
                                                      .data$tableName, 
                                                      " (n = ", 
                                                      format(big.mark = ",", scientific = FALSE, x = .data$count),
                                                      ")"
                        )) %>% 
                        dplyr::pull(.data$report) %>% 
                        sort() %>% 
                        paste0(collapse = ",")))
  ParallelLogger::logInfo(message1)
  
  # zipFiles <- tidyr::tibble(fullName = list.files(path = inputLocation, 
  #                                                 pattern = ".zip", 
  #                                                 recursive = TRUE, 
  #                                                 full.names = TRUE, 
  #                                                 ignore.case = FALSE)) %>% 
  #   dplyr::mutate(name = basename(.data$fullName) %>% 
  #                   stringr::str_replace(string = .,
  #                                        pattern = ".zip",
  #                                        replacement = ""))
  
  files <- csvFiles$tableName %>% unique() %>% sort()
  for (i in (1:length(files))) {
    csvFile <- csvFiles %>% 
      dplyr::filter(.data$tableName %in% files[[i]])
    ParallelLogger::logInfo("  Reading csv file(s) ", files[[i]], ".csv")
    
    filesRead <- list()
    for (j in (1:nrow(csvFile))) {
      ParallelLogger::logInfo("    reading ", csvFile$fullName[[j]])
      data <- readr::read_csv(file = csvFile$fullName[[j]],
                              col_types = readr::cols(),
                              guess_max = min(1e7),
                              locale = readr::locale(encoding = "UTF-8")) %>%
        dplyr::distinct()
      
      if (nrow(data) > 0) {
        expectedColNames <- resultsDataModelSpecification %>% 
          dplyr::filter(.data$tableName == files[[i]]) %>% 
          dplyr::pull(.data$fieldName) %>% 
          tolower()
        observeredColNames <- colnames(data) %>% 
          tolower()
        
        if (length(intersect(x = expectedColNames, y = observeredColNames)) != 
            length(expectedColNames)) {
          ParallelLogger::logWarn("Problem with file ", csvFile$fullName[[j]])
          ParallelLogger::logInfo("  - Agree with expected columns:", 
                                  intersect(x = expectedColNames, y = observeredColNames) %>% 
                                    paste0(collapse = ","))
          ParallelLogger::logInfo("  - Do not agree with expected columns:", 
                                  setdiff(y = expectedColNames, x = observeredColNames) %>% 
                                    paste0(collapse = ","))
          if (!setequal(x = expectedColNames,
                        y = observeredColNames)) {
            ParallelLogger::logWarn("Mismatch between expected and observered. ")
            stop()
          }
        }
        colnames(data) <- tolower(colnames(data))
        filesRead[[j]] <- data
      } else {
        ParallelLogger::logWarn(files[[i]], " has 0 records.")
        filesRead[[j]] <- tidyr::tibble()
      }
      if (('valid_start_date' %in% colnames(filesRead[[j]]) ||
           'valid_end_date' %in% colnames(filesRead[[j]])) &&
          typeof(filesRead[[j]]$valid_start_date) == 'character') {
        filesRead[[j]]$valid_end_date <- as.Date(filesRead[[j]]$valid_end_date)
        filesRead[[j]]$valid_start_date <- as.Date(filesRead[[j]]$valid_start_date)
      }
    }
    filesRead <- dplyr::bind_rows(filesRead) %>%
      dplyr::distinct()
    
    if (length(colnames(filesRead)) == 0) {
      ParallelLogger::logWarn("None of files read have any column names, are those files corrupted?")
      stop()
    }
    
    if (files[[i]] == 'concept') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$concept_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        ParallelLogger::logWarn(" Table has duplicate rows, only first occurrence is retained. 
                                \n    Please check data quality. Removed records = ", 
                                format(big.mark = ",", scientific = FALSE, x = difference, accuracy = 0))
      }
    }
    if (files[[i]] == 'analysis_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$analysis_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        ParallelLogger::logWarn(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'cohort') {
      n <- nrow(filesRead)
      filesRead <- filesRead %>% 
        dplyr::group_by(.data$referent_concept_id, .data$cohort_id,
                        .data$web_api_cohort_id) %>% 
        dplyr::slice(1)
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        ParallelLogger::logWarn(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'covariate_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$covariate_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        ParallelLogger::logWarn(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'temporal_analysis_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$analysis_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        ParallelLogger::logWarn(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'temporal_covariate_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$covariate_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        ParallelLogger::logWarn(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'temporal_time_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$time_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        ParallelLogger::logWarn(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    
    outputCsv <- file.path(outputLocation, paste0(files[[i]], ".csv"))
    readr::write_excel_csv(x = filesRead, 
                           path = outputCsv, 
                           na = '')
  }
  return(NULL)
}
