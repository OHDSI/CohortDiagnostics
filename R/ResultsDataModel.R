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
    
    fields <- table$fieldName
    script <- c(script, paste0("--Number of fields in table ", length(fields), '\n'))
    hint <- "--HINT DISTRIBUTE ON RANDOM\n"
    script <- c(script, hint, paste0("CREATE TABLE @resultsDatabaseSchema.", tableList[[i]], " (\n"))
    end <- length(fields)
    
    a <- c()
    for (f in fields) {
      if (subset(table, fieldName == f, isRequired) == "Yes") {
        r <- (" NOT NULL")
      } else {
        r <- (" NULL")
      }
      if (f == fields[[end]]) {
        e <- (" );")
      } else {
        e <- (",")
      }
      a <- c(a, paste0("\n\t\t\t",f," ",subset(table, fieldName == f, dataClass),r,e))
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
  script <- c(script, paste0("--DDL Primary Key Constraints Specification for package ", packageName, " package version: ", packageVersion, '\n'))
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
  script <- c(script, paste0("--DDL Drop table Specification for package ", packageName, " package version: ", packageVersion, '\n'))
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



#' Guesses data model specification for a csv file and makes its available for inspection.
#'
#' @param pathToCsvFile file path to csv file
#' 
#' @export
#' 
guessModelSpecificationForCsv <- function(pathToCsvFile) {
  tableToWorkOn <- basename(pathToCsvFile) %>% stringr::str_remove(string = ., pattern = ".csv")
  print(paste0("Reading csv file '", tableToWorkOn, "' and guessing data types."))
  
  csvFile <- readr::read_csv(file = pathToCsvFile,
                             col_types = readr::cols(),
                             guess_max = 1e7,
                             locale = readr::locale(encoding = "UTF-8"))
  if (any(stringr::str_detect(string = colnames(csvFile), pattern = "_"))) {
    colnames(csvFile) <- tolower(colnames(csvFile))
  }
  
  patternThatIsNotPrimaryKey = c("subjects", "entries", "name", "sql", "json", "description", "atlas_id")
  patternThatIsPrimaryKey = c('cohort_id', 'database_id', 'cohort_definition_id', 'mode_id', 'rule_sequence', 'phenotype_id')
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
    dataClass <- fieldData %>% dplyr::pull(1) %>% class() %>% max()
    if (dataClass == 'numeric') {
      if (stringr::str_detect(string = fieldName, pattern = '_id')) {
        dataClass = utils::type.convert(fieldData %>% dplyr::pull(1)) %>% class()
      }
      if (dataClass == 'numeric') {
        dataClass = 'float'
      }
    }
    if (dataClass == 'character') {
      fieldCharLength <- fieldData %>% 
        dplyr::pull(1) %>% 
        stringr::str_replace_na(string = ., replacement = '') %>% 
        stringr::str_length(string = .) %>% max()
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
      dataClass = paste0('varchar(', fieldChar, ')')
    }
    if (any(dataClass %in% c("POSIXct", "POSIXt"))) {
      dataClass <- 'Date'
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
                                   dataClass = dataClass,
                                   isRequired = isRequired,
                                   primaryKey = primaryKey)
    
    if (describe[[i]]$dataClass == 'logical') {
      describe[[i]]$dataClass == 'varchar(1)'
    }
    if (describe[[i]]$tableName == 'cohort' && 
        describe[[i]]$fieldName == 'cohort_name' &&
        describe[[i]]$dataClass == 'float') {
      describe[[i]]$dataClass = 'varchar(255)'
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