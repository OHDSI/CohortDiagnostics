# Copyright 2022 Observational Health Data Sciences and Informatics
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


annotationFunction <- function(diagnosticsId) {
  return(
    shinydashboard::box(
      title = "",
      width = NULL,
      collapsible = TRUE,
      collapsed = FALSE,
      reactable::reactableOutput(
        outputId = paste0("output", diagnosticsId),
        width = NULL
      ),
      tags$style(
        paste0(
          "#output",
          diagnosticsId,
          " {max-height:300px;overflow:auto;padding-left:30px;margin:0 0 30px 10px;border-left:1px solid #eee;}"
        )
      ),
      shiny::conditionalPanel(
        condition = "output.postAnnotationEnabled == true",
        shinydashboard::box(
          title = "Comments",
          width = NULL,
          collapsible = TRUE,
          collapsed = TRUE,
          column(
            5,
            shinyWidgets::pickerInput(
              inputId = paste0("database", diagnosticsId),
              label = "Related Database:",
              width = 300,
              choices = c(""),
              selected = c(""),
              multiple = TRUE,
              inline = TRUE,
              choicesOpt = list(style = rep_len("color: black;", 999)),
              options = shinyWidgets::pickerOptions(
                actionsBox = TRUE,
                liveSearch = TRUE,
                size = 10,
                liveSearchStyle = "contains",
                liveSearchPlaceholder = "Type here to search",
                virtualScroll = 50
              )
            )
          ),
          column(
            5,
            shinyWidgets::pickerInput(
              inputId = paste0("cohort", diagnosticsId),
              label = "Related Cohorts",
              width = 300,
              choices = c(""),
              selected = c(""),
              multiple = TRUE,
              inline = TRUE,
              choicesOpt = list(style = rep_len("color: black;", 999)),
              options = shinyWidgets::pickerOptions(
                actionsBox = TRUE,
                liveSearch = TRUE,
                liveSearchStyle = "contains",
                size = 10,
                dropupAuto = TRUE,
                liveSearchPlaceholder = "Type here to search",
                virtualScroll = 50
              )
            )
          ),
          column(
            11,
            markdownInput::markdownInput(
              inputId = paste0("annotation", diagnosticsId),
              label = "Comment : ",
              theme = "github",
              value = "Write some _markdown_ **here:**"
            )
          ),
          column(
            1,
            tags$br(),
            shiny::actionButton(
              inputId = paste0("postAnnotation", diagnosticsId),
              label = "POST",
              width = NULL,
              style = "margin-top: 15px; margin-bottom: 15px;"
            )
          )
        )
      )
    )
  )
}

postAnnotationResult <- function(dataSource,
                                 resultsDatabaseSchema,
                                 diagnosticsId,
                                 cohortIds,
                                 databaseIds,
                                 annotation,
                                 createdBy,
                                 createdOn = getTimeAsInteger(),
                                 modifiedOn = NULL,
                                 deletedOn = NULL) {

  # Prevent potential sql injection
  annotation <- gsub("'", "`", annotation)
  sqlInsert <- "INSERT INTO @results_database_schema.annotation (
                                                          	annotation_id,
                                                          	created_by,
                                                          	created_on,
                                                          	modified_last_on,
                                                          	deleted_on,
                                                          	annotation
                                                          	)
                SELECT CASE
                		WHEN max(annotation_id) IS NULL
                			THEN 1
                		ELSE max(annotation_id) + 1
                		END AS annotation_id,
                	'@created_by' created_by,
                	@created_on created_on,
                	@modified_last_on modified_last_on,
                	@deleted_on deleted_on,
                	'@annotation' annotation
                FROM @results_database_schema.annotation;"
  
  tryCatch({
    renderTranslateExecuteSql(
      connection = dataSource$connection,
      sql = sqlInsert,
      results_database_schema = dataSource$resultsDatabaseSchema,
      annotation = annotation,
      created_by = createdBy,
      created_on = createdOn,
      modified_last_on = modifiedOn,
      deleted_on = deletedOn
    )
  }, error = function(err) {
    stop(paste("Error while posting the comment, \nDescription:", err))
  })
  
  # get annotation id
  sqlRetrieve <- "SELECT max(annotation_id) annotation_id
                  FROM @results_database_schema.annotation
                  WHERE created_by = '@created_by'
                  	AND created_on = @created_on;"
  maxAnnotationId <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlRetrieve,
      results_database_schema = dataSource$resultsDatabaseSchema,
      created_by = createdBy,
      created_on = createdOn
    ) %>% dplyr::pull()
  
  # insert annotation link
  annotationLink <-
    tidyr::crossing(
      annotationId = !!maxAnnotationId,
      diagnosticsId = !!diagnosticsId,
      cohortId = !!cohortIds,
      databaseId = !!databaseIds
    )
  realConnection <- pool::poolCheckout(dataSource$connection)
  DatabaseConnector::insertTable(
    connection = realConnection,
    databaseSchema = dataSource$resultsDatabaseSchema,
    tableName = "annotation_link",
    createTable = FALSE,
    dropTableIfExists = FALSE,
    tempTable = FALSE,
    progressBar = FALSE,
    camelCaseToSnakeCase = TRUE,
    data = annotationLink
  )
  pool::poolReturn(realConnection)
  return(TRUE)
}


getAnnotationResult <- function(dataSource,
                                diagnosticsId,
                                cohortIds,
                                databaseIds) {
  # get annotation id's
  sqlRetrieveAnnotationLink <- "SELECT *
                                FROM @results_database_schema.annotation_link
                                WHERE diagnostics_id = '@diagnosticsId'
                                	AND cohort_id IN (@cohortIds)
                                  AND database_id IN (@databaseIds);"
  annotationLink <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlRetrieveAnnotationLink,
      results_database_schema = dataSource$resultsDatabaseSchema,
      diagnosticsId = diagnosticsId,
      cohortIds = cohortIds,
      databaseIds = quoteLiterals(databaseIds),
      snakeCaseToCamelCase = TRUE
    )
  
  sqlRetrieveAnnotation <- "SELECT *
                            FROM @results_database_schema.annotation
                            WHERE annotation_id IN (@annotationIds);"
  
  annotation <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlRetrieveAnnotation,
      results_database_schema = dataSource$resultsDatabaseSchema,
      annotationIds = annotationLink$annotationId,
      snakeCaseToCamelCase = TRUE
    )
  
  data <- list(annotation = annotation,
               annotationLink = annotationLink)
  
  return(data)
}
