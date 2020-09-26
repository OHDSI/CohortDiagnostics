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
    for (f in (1:length(fields))) { 
      #from https://github.com/OHDSI/CdmDdlBase/blob/f256bd2a3350762e4a37108986711516dd5cd5dc/R/createDdlFromFile.R#L50
      field <- fields[[f]]
      if (table %>% dplyr::filter(.data$fieldName == !!field) %>% dplyr::pull(.data$isRequired) == "Yes") {
        r <- (" NOT NULL")
      } else {
        r <- (" NULL")
      }
      if (field == fields[[length(fields)]]) {
        e <- (" );")
      } else {
        e <- (",")
      }
      a <- c(a, paste0("\n\t\t\t",field," ",
                       table %>% dplyr::filter(.data$fieldName == !!field) %>% dplyr::pull(.data$type), 
                       r, e))
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



#' Prepare diagnostics results for use
#' 
#' @description 
#' This function takes as input one or more .zip files that are the output of
#' \code{\link{runCohortDiagnostics}} function, performs a set of data management and 
#' data quality checks. Data Management steps involve appending and depulicating data
#' sourced from different data sources (each should be a seperate zip file). Data
#' Quality checks involves checks for conformance to name/data type/data constraints
#' requirements for the Cohort Diagnostics Results data model. The function will 
#' output two files a 
#' - zip file: a compressed zip file with csv that is ready for upload into a rdms 
#' system with the schema/table/constraints compatible with the Cohort Diagnostics
#' results data model (see '/settings/resultsDataModelSpecification.csv')
#' - rds file: called preMerged.RData that may used in the data folder of a Shiny 
#' app. This rds data when placed in the 'data' folder of the shiny app will 
#' be automatically recognized by the Shiny app.
#' 
#' Note: the csv files in the .zip files are expected to have a snake case file
#' naming convention and snake case column names.
#'
#' @param dataFolder       folder where the exported zip files for the diagnostics are stored. Use
#'                         the \code{\link{runCohortDiagnostics}} function to generate these zip files. 
#'                         Zip files containing results from multiple databases may be placed in the same
#'                         folder. 
#' @param outputFolder     folder where the post processed files for the diagnostics are to be stored. 
#'                         These files may be used with the results viewer or may be uploaded into RDMS.
#' @export
postProcessDiagnosticsResultsFiles <- function(dataFolder,
                                               outputFolder) {
  zipFiles <- list.files(dataFolder, pattern = ".zip", full.names = TRUE, recursive = TRUE)
  ParallelLogger::logInfo("Found ", length(zipFiles), " zip files.")
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertVector(x = zipFiles, any.missing = FALSE, min.len = 1, add = errorMessage)
  checkmate::assertFileExists(x = system.file('settings',
                                              'resultsDataModelSpecification.csv',
                                              package = 'CohortDiagnostics'), 
                              add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  
  # find and unzip files to temporary location
  temporayFileLocation <- file.path(tempdir(), "1")
  temporayFileLocationList <- list()
  for (i in (1:length(zipFiles))) {
    temporayFileLocationList[[i]] <- file.path(temporayFileLocation, i)
    unlink(x = temporayFileLocationList[[i]],
           recursive = TRUE, 
           force = TRUE)
    dir.create(path = temporayFileLocationList[[i]],showWarnings = FALSE, recursive = TRUE)
    ParallelLogger::logInfo("Unzipping ", zipFiles[i])
    unzip(zipfile = zipFiles[i], exdir = temporayFileLocationList[[i]], overwrite = TRUE)
  }
  
  resultsDataModelSpecification <- 
    readr::read_csv(file = system.file('settings',
                                       'resultsDataModelSpecification.csv',
                                       package = 'CohortDiagnostics'),
                    col_types = readr::cols(), 
                    guess_max = min(1e7), 
                    locale = readr::locale(encoding = "UTF-8"))
  
  csvFiles <- dplyr::tibble(fullName = list.files(path = temporayFileLocation, 
                                                  pattern = ".csv", 
                                                  recursive = TRUE, 
                                                  full.names = TRUE, 
                                                  ignore.case = FALSE)) %>% 
    dplyr::mutate(tableName = stringr::str_replace(string = basename(.data$fullName),
                                                   pattern = ".csv",
                                                   replacement = "")) %>% 
    dplyr::inner_join(resultsDataModelSpecification %>% 
                        dplyr::select(.data$tableName) %>% 
                        dplyr::distinct()
    )
  
  files <- csvFiles$tableName %>% unique() %>% sort()
  unlink(file.path(outputFolder, 'csv'))
  for (i in (1:length(files))) {
    file <- files[[i]]
    csvFile <- csvFiles %>% 
      dplyr::filter(.data$tableName %in% files[[i]])
    ParallelLogger::logInfo("Combining ", nrow(csvFile), " of ", file, ".csv files into one.")
    
    filesRead <- list()
    for (j in (1:nrow(csvFile))) {
      data <- readr::read_csv(file = csvFile$fullName[[j]],
                              col_types = readr::cols(.default = "c"),
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
        
        if (length(intersect(x = expectedColNames, 
                             y = observeredColNames)) != 
            length(expectedColNames)) {
          warning("Problem with file ", csvFile$fullName[[j]])
          ParallelLogger::logInfo("  - Agree with expected columns:", 
                                  intersect(x = expectedColNames, y = observeredColNames) %>% 
                                    paste0(collapse = ","))
          ParallelLogger::logInfo("  - Do not agree with expected columns:", 
                                  setdiff(y = expectedColNames, x = observeredColNames) %>% 
                                    paste0(collapse = ","))
          if (!setequal(x = expectedColNames,
                        y = observeredColNames)) {
            warning("Mismatch between expected and observered. ")
            stop()
          }
        }
        filesRead[[j]] <- data
      } else {
        warning(files[[i]], " has 0 records.")
        filesRead[[j]] <- dplyr::tibble()
      }
    }
    filesRead <- dplyr::bind_rows(filesRead) %>%
      dplyr::distinct()
    
    if (length(colnames(filesRead)) == 0) {
      warning("None of files read have any column names, are those files corrupted?")
      stop()
    }
    #TODO change the code below to check for duplicates by primary key(s) in results data model
    #TODO if its violated for any reasons other than vocabulary - then reject the data.
    if (files[[i]] == 'concept') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$concept_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        warning(" Table has duplicate rows, only first occurrence is retained. 
                                \n    Please check data quality. Removed records = ", 
                format(big.mark = ",", scientific = FALSE, x = difference, accuracy = 0))
      }
    }
    if (files[[i]] == 'analysis_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$analysis_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        warning(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
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
        warning(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'covariate_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$covariate_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        warning(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'temporal_analysis_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$analysis_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        warning(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'temporal_covariate_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$covariate_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        warning(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    if (files[[i]] == 'temporal_time_ref') {
      n <- nrow(filesRead)
      filesRead <- filesRead[!duplicated(filesRead$time_id),]
      difference <- (nrow(filesRead)  - n)
      if (difference > 0) {
        warning(" Table has duplicate rows, only first occurrence is retained. Please check data quality. Removed records = ", format(big.mark = ",", scientific = FALSE, x = difference))
      }
    }
    dir.create(path = file.path(outputFolder, 'csv'), showWarnings = FALSE, recursive = TRUE)
    outputCsv <- file.path(outputFolder, 'csv', paste0(files[[i]], ".csv"))
    readr::write_excel_csv(x = filesRead, 
                           path = outputCsv, 
                           na = '')
  }
  ParallelLogger::logInfo("Creating zip file, this may take some time....")
  DatabaseConnector::createZipFile(zipFile = file.path(outputFolder, 'combined.zip'), 
                                   files = file.path(outputFolder, 'csv'),
                                   rootFolder = file.path(outputFolder, 'csv')
  )
  
  unlink(x = temporayFileLocationList, recursive = TRUE, force = TRUE)
  
  return(NULL)
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
