# library(dplyr)
# 
# guessCsvFileSpecification <- function(pathToCsvFile) {
#   tableToWorkOn <-  stringr::str_remove(string = basename(pathToCsvFile), 
#                                         pattern = ".csv")
#   
#   print(paste0("Reading csv files '", tableToWorkOn, "' and guessing data types."))
#   
#   csvFile <- readr::read_csv(file = pathToCsvFile,
#                              col_types = readr::cols(),
#                              guess_max = min(1e7),
#                              locale = readr::locale(encoding = "UTF-8"))
#   if (any(stringr::str_detect(string = colnames(csvFile), pattern = "_"))) {
#     colnames(csvFile) <- tolower(colnames(csvFile))
#   }
#   
#   patternThatIsNotPrimaryKey = c("subjects", "entries", "name", "sql", "json", "description", "atlas_id", "day")
#   patternThatIsPrimaryKey = c('_id', 'rule_sequence')
#   describe <- list()
#   primaryKeyIfOmopVocabularyTable <-  getPrimaryKeyForOmopVocabularyTable() %>% 
#     dplyr::filter(.data$vocabularyTableName == tableToWorkOn %>% tolower()) %>% 
#     dplyr::pull(.data$primaryKey) %>% 
#     strsplit(split = ",") %>% 
#     unlist() %>% 
#     tolower()
#   
#   for (i in (1:length(colnames(csvFile)))) {
#     tableName <- tableToWorkOn
#     fieldName <- colnames(csvFile)[[i]]
#     fieldData <- csvFile %>% dplyr::select(fieldName)
#     dataVector <- fieldData %>% dplyr::pull(1)
#     type <- suppressWarnings(guessDbmsDataTypeFromVector(value = dataVector))
#     if (stringr::str_detect(string = fieldName, 
#                             pattern = stringr::fixed('_id')) &&
#         type == 'float') {
#       type = 'bigint'
#     }
#     if (stringr::str_detect(string = tolower(fieldName), 
#                             pattern = stringr::fixed('description')) &&
#         (stringr::str_detect(string = type, 
#                              pattern = 'varchar') ||
#          stringr::str_detect(string = type, 
#                              pattern = 'logical')
#         )
#     ) {
#       type = 'varchar(max)'
#     }
#     isRequired <- 'Yes'
#     if (anyNA(csvFile %>% dplyr::pull(fieldName))) {
#       isRequired <- 'No'
#     }
#     primaryKey <- 'No'
#     if (tableName %in% getPrimaryKeyForOmopVocabularyTable()$vocabularyTableName && 
#         fieldName %in% primaryKeyIfOmopVocabularyTable) {
#       primaryKey <- 'Yes'
#     } else if (isRequired == 'Yes' &&
#                nrow(csvFile) == nrow(csvFile %>% dplyr::select(fieldName) %>% dplyr::distinct()) &&
#                all(stringr::str_detect(string = fieldName, 
#                                        pattern = patternThatIsNotPrimaryKey, 
#                                        negate = TRUE))) {
#       primaryKey <- 'Yes'
#     } else if (isRequired == 'Yes' &&
#                any(stringr::str_detect(string = fieldName, 
#                                        pattern = patternThatIsPrimaryKey))) {
#       primaryKey <- 'Yes'
#     }
#     describe[[i]] <- tidyr::tibble(tableName = tableName, 
#                                    fieldName = fieldName,
#                                    type = type,
#                                    isRequired = isRequired,
#                                    primaryKey = primaryKey)
#     
#     if (describe[[i]]$type == 'logical') {
#       describe[[i]]$type == 'varchar(1)'
#     }
#     if (describe[[i]]$tableName == 'cohort' && 
#         describe[[i]]$fieldName == 'cohort_name' &&
#         describe[[i]]$type == 'float') {
#       describe[[i]]$type = 'varchar(255)'
#     }
#     if (describe[[i]]$tableName == 'incidence_rate' && describe[[i]]$fieldName == 'calendar_year') {
#       describe[[i]]$primaryKey = 'Yes'
#     }
#     if (describe[[i]]$tableName %in% c('covariate_value', 'temporal_covariate_value', 'time_distribution') && 
#         describe[[i]]$fieldName %in% c('covariate_id','start_day','end_day')) {
#       describe[[i]]$primaryKey = 'Yes'
#     }
#     if (describe[[i]]$tableName %in% c('included_source_concept','index_event_breakdown', 'orphan_concept')) {
#       if (describe[[i]]$fieldName %in% c('concept_set_id', 'concept_id', 'source_concept_id')) {
#         describe[[i]]$primaryKey = 'Yes'
#       }
#     }
#   }
#   describe <- dplyr::bind_rows(describe)
#   return(describe)
# }

createDdl <- function(fileName, 
                      specifications = CohortDiagnostics::getResultsDataModelSpecifications()){
  tableNames <- specifications$tableName %>% unique()
  script <- c()
  script <- c(script, "-- Drop old tables if exist")
  script <- c(script, "")
  for (tableName in tableNames) {
    script <- c(script, paste0("DROP TABLE IF EXISTS ", tableName, ";"))
  }
  script <- c(script, "")
  script <- c(script, "")
  script <- c(script, "-- Create tables")
  for (tableName in tableNames) {
    script <- c(script, "")
    script <- c(script, paste("--Table", tableName))
    script <- c(script, "")
    table <- specifications %>% 
      dplyr::filter(.data$tableName == !!tableName)
    
    script <- c(script, paste0("CREATE TABLE ", tableName, " ("))
    fieldSql <- c()
    for (fieldName in table$fieldName) { 
      field <- table %>%
        filter(.data$fieldName == !!fieldName)
      
      if (field$primaryKey == "Yes") {
        required <- " PRIMARY KEY"
      } 
      
      if (field$isRequired == "Yes") {
        required <- " NOT NULL"
      } else {
        required = "" 
      }
      fieldSql <- c(fieldSql, paste0("\t\t\t", 
                                     fieldName, 
                                     " ",
                                     toupper(field$type),
                                     required))
    }
    primaryKeys <- table %>%
      filter(.data$primaryKey == "Yes") %>%
      select(.data$fieldName) %>%
      pull()
    fieldSql <- c(fieldSql, paste0("\t\t\tPRIMARY KEY(", paste(primaryKeys, collapse = ", "), ")")) 
    script <- c(script, paste(fieldSql, collapse = ",\n"))
    script <- c(script, ");")
  }
  SqlRender::writeSql(paste(script, collapse = "\n"), fileName)
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

