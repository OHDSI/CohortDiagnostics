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
#

# Concept search using string

#' Get concepts that match a string search
#'
#' @template Connection
#'
#' @template VocabularyDatabaseSchema
#'
#' @param searchString A phrase (can be multiple words) to search for.
#'
getStringSearchConcepts <-
  function(searchString,
           vocabularyDatabaseSchema = 'vocabulary',
           connection = NULL,
           connectionDetails = NULL,
           conceptPrevalenceSchema = 'concept_prevalence') {
    warning("Running experimental function")
    searchString <-
      stringr::str_squish(tolower(gsub("[^a-zA-Z0-9 ,]", " ", searchString)))
    
    if ( (!is.null(connection) && attr(connection, "dbms") == 'postgresql') ||
         connectionDetails$dbms == 'postgresql' ||
         methods::is(connection, "Pool")) {
      # Note this function is designed for postgres with TSV enabled.
      # Filtering strings to letters, numbers and spaces only to avoid SQL injection
      # also making search string of lower case - to make search uniform.
      
      # reversing for reverse search in TSV
      searchStringReverse <-
        stringi::stri_reverse(str = searchString)
      ## if any string is shorter than 5 letters than it brings back
      ## non specific search result
      searchStringReverse <-
        stringr::str_split(string = searchStringReverse, pattern = " ") %>% unlist()
      for (i in (1:length(searchStringReverse))) {
        if (nchar(searchStringReverse[[i]]) < 5) {
          searchStringReverse[[i]] <- ''
        }
      }
      searchStringReverse <-
        stringr::str_squish(paste(searchStringReverse, collapse = " "))
      
      # function to create TSV string for post gres
      stringForTsvSearch <- function(string) {
        string <- stringr::str_squish(string)
        # split the string to vector
        stringSplit = strsplit(x = string, split = " ") %>% unlist()
        # add wild card only if word is atleast three characters long
        for (i in (1:length(stringSplit))) {
          if (nchar(stringSplit[[i]]) > 2) {
            stringSplit[[i]] <- paste0(stringSplit[[i]], ':*')
          }
        }
        return(paste(stringSplit, collapse = " & "))
      }
      
      searchStringTsv <-
        if (searchString != '') {
          stringForTsvSearch(searchString)
        } else {
          searchString
        }
      searchStringReverseTsv <-
        if (searchStringReverse != '') {
          stringForTsvSearch(searchStringReverse)
        } else {
          searchStringReverse
        }
      
      sql <-
        SqlRender::readSql(
          sourceFile = system.file(
            "sql",
            "sql_server",
            'SearchVocabularyForConceptsPostGresTsv.sql',
            package = "ConceptSetDiagnostics"
          )
        )
      
      data <-
        renderTranslateQuerySql(
          connection = connection,
          connectionDetails = connectionDetails,
          sql = sql,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          concept_prevalence_schema = conceptPrevalenceSchema,
          search_string_tsv = searchStringTsv,
          search_string_reverse_tsv = searchStringReverseTsv,
          search_string = searchString,
          snakeCaseToCamelCase = TRUE
        )
    } else {
      sql <-
        SqlRender::readSql(
          sourceFile = system.file(
            "sql",
            "sql_server",
            'SearchVocabularyForConcepts.sql',
            package = "ConceptSetDiagnostics"
          )
        )
      
      data <-
        renderTranslateQuerySql(
          connection = connection,
          connectionDetails = connectionDetails,
          sql = sql,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          concept_prevalence_schema = conceptPrevalenceSchema,
          search_string = searchString,
          snakeCaseToCamelCase = TRUE
        )
    }
    return(data)
  }
