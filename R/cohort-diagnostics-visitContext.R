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

#' Visit context module view
#' @description
#' Use for customizing UI
#'
#' @param id        Namespace Id - use namespaced id ns("vistConext") inside diagnosticsExplorer module
#' @family CohortDiagnostics
#' @export
visitContextView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Visit Context",
      width = "100%",
      shiny::htmlTemplate(system.file("cohort-diagnostics-www",  "visitContext.html", package = utils::packageName()))
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
      width = NULL,
      title = NULL,
      shiny::tags$table(
        width = "100%",
        shiny::tags$tr(
          shiny::tags$td(
            shiny::radioButtons(
              inputId = ns("visitContextTableFilters"),
              label = "Display",
              choices = c("All", "Before", "During", "Simultaneous", "After"),
              selected = "All",
              inline = TRUE
            )
          ),
          shiny::tags$td(
            shiny::radioButtons(
              inputId = ns("visitContextPersonOrRecords"),
              label = "Display",
              choices = c("Persons", "Records"),
              selected = "Persons",
              inline = TRUE
            )
          ),
          shiny::tags$td(
            align = "right"
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("visitContextTable"))),
      reactableCsvDownloadButton(ns, "visitContextTable")
    )
  )
}


getVisitContextResults <- function(dataSource,
                                   cohortIds,
                                   cohortCount,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <-
    checkErrorCohortIdsDatabaseIds(
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      errorMessage = errorMessage
    )
  checkmate::reportAssertions(collection = errorMessage)

  sql <- "SELECT visit_context.*,
              standard_concept.concept_name AS visit_concept_name
            FROM  @schema.@table_name visit_context
            INNER JOIN  @vocabulary_database_schema.@concept_table standard_concept
              ON visit_context.visit_concept_id = standard_concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
  data <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("visit_context"),
      concept_table = dataSource$prefixVocabTable("concept"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c("cohortId", "databaseId")
    ) %>%
    dplyr::mutate(subjectPercent = .data$subjects / .data$cohortSubjects)
  return(data)
}

visitContextModule <- function(id = "visitContext",
                               dataSource,
                               selectedCohort, #this is selectedCohorts in other modules
                               selectedDatabaseIds,
                               targetCohortId,
                               cohortCountTable = dataSource$cohortCountTable,
                               databaseTable = dataSource$dbTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI(selectedCohort())
    
    # Visit Context ----------------------------------------
    getVisitContextData <- shiny::reactive(x = {
      if (!hasData(selectedDatabaseIds())) {
        return(NULL)
      }
      if (all(inherits(dataSource, "environment"), !exists("visitContext"))) {
        return(NULL)
      }
      visitContext <-
        getVisitContextResults(
          dataSource = dataSource,
          cohortIds = targetCohortId(),
          cohortCount = dataSource$cohortCountTable,
          databaseIds = selectedDatabaseIds()
        )
      if (!hasData(visitContext)) {
        return(NULL)
      }
      return(visitContext)
    })

    ## getVisitContexDataEnhanced----
    getVisitContexDataEnhanced <- shiny::reactive(x = { #spelling error here missing the t in Context
      visitContextData <- getVisitContextData()
      if (!hasData(visitContextData)) {
        return(NULL)
      }

      visitContextData <- visitContextData %>%
        dplyr::rename("visitContextSubject" = "subjects")

      visitContextData <-
        expand.grid(
          visitContext = c("Before", "During visit", "On visit start", "After"),
          visitConceptName = unique(visitContextData$visitConceptName),
          databaseId = unique(visitContextData$databaseId),
          cohortId = unique(visitContextData$cohortId)
        ) %>%
          dplyr::tibble() %>%
          dplyr::left_join(
            visitContextData,
            by = c(
              "visitConceptName",
              "visitContext",
              "databaseId",
              "cohortId"
            )
          ) %>%
          dplyr::rename(
            "subjects" = "cohortSubjects",
            "records" = "cohortEntries"
          ) %>%
          dplyr::select(
            "databaseId",
            "cohortId",
            "visitConceptName",
            "visitContext",
            "subjects",
            "records",
            "visitContextSubject"
          ) %>%
          dplyr::mutate(
            visitContext = dplyr::case_when(
              visitContext == "During visit" ~ "During",
              visitContext == "On visit start" ~ "Simultaneous",
              TRUE ~ visitContext
            )
          ) %>%
          tidyr::replace_na(replace = list(subjects = 0, records = 0))


      if (input$visitContextTableFilters == "Before") {
        visitContextData <- visitContextData %>%
          dplyr::filter(.data$visitContext == "Before")
      } else if (input$visitContextTableFilters == "During") {
        visitContextData <- visitContextData %>%
          dplyr::filter(.data$visitContext == "During")
      } else if (input$visitContextTableFilters == "Simultaneous") {
        visitContextData <- visitContextData %>%
          dplyr::filter(.data$visitContext == "Simultaneous")
      } else if (input$visitContextTableFilters == "After") {
        visitContextData <- visitContextData %>%
          dplyr::filter(.data$visitContext == "After")
      }
      if (!hasData(visitContextData)) {
        return(NULL)
      }
      visitContextData <- visitContextData %>%
        tidyr::pivot_wider(
          id_cols = c("databaseId", "visitConceptName"),
          names_from = "visitContext",
          values_from = c("visitContextSubject")
        )
      
      return(visitContextData)
    })

    output$visitContextTable <- reactable::renderReactable(expr = {
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(length(targetCohortId()) > 0, "No cohorts chosen"))
      data <- getVisitContexDataEnhanced()
      shiny::validate(shiny::need(
        nrow(data) > 0,
        "No data available for selected combination."
      ))
  
      dataColumnFields <-
        c(
          "Before",
          "During",
          "Simultaneous",
          "After"
        )

      if (input$visitContextTableFilters == "Before") {
        dataColumnFields <- "Before"
      } else if (input$visitContextTableFilters == "During") {
        dataColumnFields <- "During"
      } else if (input$visitContextTableFilters == "Simultaneous") {
        dataColumnFields <- "Simultaneous"
      } else if (input$visitContextTableFilters == "After") {
        dataColumnFields <- "After"
      }
      keyColumnFields <- "visitConceptName"
      
      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = targetCohortId(),
          source = "cohort",
          fields = input$visitContextPersonOrRecords
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }

      maxCountValue <-
        getMaxValByString(
          data = data,
          string = dataColumnFields
        )
    
      getDisplayTableGroupedByDatabaseId(
        data = data,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        sort = TRUE
      )
    })
  })
}
