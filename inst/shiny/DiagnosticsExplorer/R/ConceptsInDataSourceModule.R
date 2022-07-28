conceptsInDataSourceView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      status = "warning",
      width = "100%",
      tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohorts"))
      )
    ),
    shinydashboard::box(
      title = "Concepts in Data Source",
      width = NULL,
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            shiny::radioButtons(
              inputId = ns("includedType"),
              label = "",
              choices = c("Source fields", "Standard fields"),
              selected = "Standard fields",
              inline = TRUE
            )
          ),
          tags$td(
            shiny::radioButtons(
              inputId = ns("conceptsInDataSourceTableColumnFilter"),
              label = "",
              choices = c("Both", "Persons", "Records"),
              #
              selected = "Persons",
              inline = TRUE
            )
          )
        ),
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("conceptsInDataSourceTable"))),
      csvDownloadButton(ns, "conceptsInDataSourceTable")
    )
  )
}


conceptsInDataSourceModule <- function(id,
                                       dataSource,
                                       selectedCohort,
                                       selectedDatabaseIds,
                                       targetCohortId,
                                       selectedConceptSets,
                                       getFilteredConceptIds,
                                       cohortTable,
                                       databaseTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI({ selectedCohort() })
    # Concepts in data source------
    conceptsInDataSourceReactive <- shiny::reactive(x = {
      validate(need(
        all(!is.null(selectedDatabaseIds()), length(selectedDatabaseIds()) > 0),
        "No data sources chosen"
      ))
      validate(need(
        all(!is.null(targetCohortId()), length(targetCohortId()) > 0),
        "No cohort chosen"
      ))
      data <- getConceptsInCohort(
        dataSource = dataSource,
        cohortId = targetCohortId(),
        databaseIds = selectedDatabaseIds()
      )
      return(data)
    })

    output$conceptsInDataSourceTable <- reactable::renderReactable(expr = {
      validate(need(hasData(selectedDatabaseIds()), "No cohort chosen"))
      validate(need(hasData(targetCohortId()), "No cohort chosen"))

      data <- conceptsInDataSourceReactive()
      validate(need(
        hasData(data),
        "No data available for selected combination"
      ))
      if (hasData(selectedConceptSets())) {
        if (length(getFilteredConceptIds()) > 0) {
          data <- data %>%
            dplyr::filter(.data$conceptId %in% getFilteredConceptIds())
        }
      }
      validate(need(
        hasData(data),
        "No data available for selected combination"
      ))

      if (input$includedType == "Source fields") {
        data <- data %>%
          dplyr::filter(.data$conceptId > 0) %>%
          dplyr::filter(.data$sourceConceptId == 1) %>%
          dplyr::rename(standard = .data$standardConcept)
        keyColumnFields <-
          c("conceptId", "conceptName", "vocabularyId", "conceptCode")
      }
      if (input$includedType == "Standard fields") {
        data <- data %>%
          dplyr::filter(.data$conceptId > 0) %>%
          dplyr::filter(.data$sourceConceptId == 0) %>%
          dplyr::rename(standard = .data$standardConcept)
        keyColumnFields <-
          c("conceptId", "conceptName", "vocabularyId")
      }

      validate(need(hasData(data), "No data available for selected combination"))
      data <- data %>%
        dplyr::rename(
          persons = .data$conceptSubjects,
          records = .data$conceptCount
        ) %>%
        dplyr::arrange(dplyr::desc(abs(dplyr::across(c("records", "persons")))))

      if (input$conceptsInDataSourceTableColumnFilter == "Persons") {
        dataColumnFields <- c("persons")
        countLocation <- 1
      } else if (input$conceptsInDataSourceTableColumnFilter == "Records") {
        dataColumnFields <- c("records")
        countLocation <- 1
      } else {
        dataColumnFields <- c("persons", "records")
        countLocation <- 2
      }

      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = targetCohortId(),
          source = "cohort",
          fields = input$conceptsInDataSourceTableColumnFilter
        )

      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = data,
          string = dataColumnFields
        )

      showDataAsPercent <- FALSE
      ## showDataAsPercent set based on UI selection - proportion

      displayTable <- getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = showDataAsPercent,
        sort = TRUE
      )
      return(displayTable)
    })
  })
}