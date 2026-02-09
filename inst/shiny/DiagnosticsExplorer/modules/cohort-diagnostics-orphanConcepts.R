# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of PatientLevelPrediction
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

#' Orphan Concepts View
#' @description
#' Use for customizing UI
#' @family CohortDiagnostics
#' @param id    Namespace Id - use namespaced id ns("orphanConcepts") inside diagnosticsExplorer module
#' 
#' @export
orpahanConceptsView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Orphan Concepts",
      width = "100%",
      shiny::htmlTemplate(file.path(cdWwwPath, "orphanConcepts.html"))
    ),
    shinydashboard::box(
      status = "warning",
      width = "100%",
      shiny::tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohorts"))
      )
    ),
    shinydashboard::box(
      title = NULL,
      width = NULL,
      shiny::fluidRow(
        shiny::column(width = 4,
                      shiny::radioButtons(
                        inputId = ns("orphanConceptsType"),
                        label = "Filters",
                        choices = c("All" = 0, "Standard Only" = 1, "Non Standard Only" = 2),
                        selected = 0,
                        inline = TRUE
                      )
        ),
        shiny::column(width = 4,
                      shiny::radioButtons(
                        inputId = ns("orphanConceptsColumFilterType"),
                        label = "Display",
                        choices = c("All" = 0, "Persons" = 1, "Records" = 2),
                        selected = 0,
                        inline = TRUE
                      )
        ),
        shiny::column(width = 4,
                      shiny::textInput(inputId = ns("generalSearchString"),
                                       label = "",
                                       placeholder = "Search concepts")
        )
      ),
      shiny::fluidRow(
        shiny::column(width = 6,
                      shiny::selectInput(inputId = ns("sortBy"),
                                         label = "Sort By",
                                         choices = NULL)
        ),
        shiny::column(width = 2,
                      shiny::radioButtons(inputId = ns("shortByAsc"),
                                          choices = c(ascending = "ASC", descending = "DESC"),
                                          selected = "DESC",
                                          label = "order")
        )
      ),
      largeTableView(id = ns("orphanConceptsTable"), selectedPageSize = 50)
    )
  )
}

orphanConceptsModule <- function(id,
                                 dataSource,
                                 selectedCohort,
                                 selectedDatabaseIds,
                                 targetCohortId,
                                 selectedConceptSets,
                                 conceptSetIds,
                                 databaseTable = dataSource$dbTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI({ selectedCohort() })

    # Orphan concepts table --------------------
    inputButtonParams <- shiny::reactive({
      conceptSets <- conceptSetIds()
      if (length(conceptSets) == 0) {
        conceptSets <- NULL
      }

      params <- list(
        database_ids = quoteLiterals(selectedDatabaseIds()),
        cohort_id = targetCohortId(),
        concept_set_id = conceptSets,
        use_concept_set_id = length(conceptSets) > 0,
        schema = dataSource$schema,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        orphan_table_name = dataSource$prefixTable("orphan_concept"),
        cs_table_name = dataSource$prefixTable("concept_sets"),
        concept_table = dataSource$prefixVocabTable("concept"),
        search_str = input$generalSearchString,
        sort_by = input$sortBy,
        sort_by_asc = ifelse(input$shortByAsc == "DESC", "DESC", "ASC") # Prevent sql injection
      )

      return(params)
    })

    databaseSubGrp <- ",
    MAX(CASE WHEN oc.database_id = '@db_id_i' THEN oc.concept_count END) AS concept_count_@db_id_id,
    MAX(CASE WHEN oc.database_id = '@db_id_i' THEN oc.concept_subjects END) AS subject_count_@db_id_id"

    sql <- "
    SELECT
      c.concept_id,
      c.concept_name,
      c.vocabulary_id,
      c.concept_code,
      CASE WHEN c.standard_concept = 'S' THEN 'Standard' ELSE 'Non-standard' END as standard_concept
      %s
    FROM  @schema.@orphan_table_name oc
    INNER JOIN  @schema.@cs_table_name cs
      ON oc.cohort_id = cs.cohort_id
        AND oc.concept_set_id = cs.concept_set_id
    INNER JOIN  @vocabulary_database_schema.@concept_table c
      ON oc.concept_id = c.concept_id
    WHERE oc.cohort_id = @cohort_id
      AND database_id in (@database_ids)
      {@search_str != ''} ? {AND lower(CONCAT(c.concept_id, c.concept_name, c.vocabulary_id, c.concept_code)) LIKE lower('%%@search_str%%')}
      {@use_concept_set_id} ? { AND oc.concept_set_id IN (@concept_set_id)}
    GROUP BY
      c.concept_id,
      c.concept_name,
      c.vocabulary_id,
      c.concept_code,
      c.standard_concept
    {@sort_by != \"\"} ? {ORDER BY @sort_by @sort_by_asc}
      "

    shiny::observe({
      databaseIds <- selectedDatabaseIds()
      dbSelectCols <- ""

      columnDefinitions <- list(
        conceptId = reactable::colDef(name = "Concept Id"),
        conceptName = reactable::colDef(name = "Concept Name", minWidth = 200),
        vocabularyId = reactable::colDef(name = "Vocabulary Id"),
        conceptCode = reactable::colDef(name = "Concept Code"),
        standardConcept = reactable::colDef(name = "Standard Concept")
      )

      columnGroups <- list()
      sortByColumns <- c("Concept Id" = "c.concept_id",
                         "Concept Name" = "c.concept_name",
                         "Vocabulary Id" = "c.vocabulary_id",
                         "Concept Code" = "c.concept_code")
      for (dbid in databaseIds) {
        dbCols <- SqlRender::render(databaseSubGrp, db_id_i = dbid, db_id_id = gsub("-", "", dbid))
        dbSelectCols <- paste(dbSelectCols, dbCols)

        columnIdCount <- SqlRender::snakeCaseToCamelCase(paste0("concept_count_", gsub("-", "", dbid)))
        columnIdSubject <- SqlRender::snakeCaseToCamelCase(paste0("subject_count_", gsub("-", "", dbid)))

        columnDefinitions[[columnIdCount]] <- reactable::colDef(name = "Records",
                                                                cell = formatDataCellValueInDisplayTable(),
                                                                show = input$orphanConceptsColumFilterType %in% c(0,2))
        columnDefinitions[[columnIdSubject]] <- reactable::colDef(name = "Persons",
                                                                  cell = formatDataCellValueInDisplayTable(),
                                                                  show = input$orphanConceptsColumFilterType %in% c(0,1))

        databaseName <- databaseTable %>%
          dplyr::filter(.data$databaseId == dbid) %>%
          dplyr::select("databaseName") %>%
          dplyr::pull()

        columnGroups[[length(columnGroups) + 1]] <- reactable::colGroup(
          name = databaseName,
          columns = c(columnIdCount, columnIdSubject),
          align = "center"
        )

        cNames <- names(sortByColumns)
        sortByColumns <- c(sortByColumns, paste0("concept_count_", gsub("-", "", dbid)), paste0("subject_count_", gsub("-", "", dbid)))
        names(sortByColumns) <- c(cNames, paste(databaseName, "Records"), paste(databaseName, "Subjects"))
      }

      shiny::updateSelectInput(inputId = "sortBy", choices = sortByColumns, selected = sortByColumns[[1]])

      baseQuery <- sprintf(sql, dbSelectCols)
      ldt <- LargeDataTable$new(connectionHandler = dataSource$connectionHandler,
                                baseQuery = baseQuery)

      largeTableServer(id = "orphanConceptsTable",
                       ldt,
                       inputParams = inputButtonParams,
                       columns = columnDefinitions,
                       columnGroups = columnGroups)

    })

  })
}
