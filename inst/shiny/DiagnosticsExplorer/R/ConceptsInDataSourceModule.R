conceptsInDataSourceView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Concepts in Data Source",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "conceptsInDataSource.html"))
    ),
    shinydashboard::box(
      status = "warning",
      width = "100%",
      tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohorts"))
      )
    ),
    shinydashboard::box(
      title = NULL,
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

    conceptSetIds <- shiny::reactive({
      selectedConceptSets()
    })

    getResolvedConcepts <- shiny::reactive({
      output <- resolvedConceptSet(
        dataSource = dataSource,
        databaseIds = selectedDatabaseIds(),
        cohortId = targetCohortId()
      )
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })

    ### getMappedConceptsReactive ----
    getMappedConcepts <- shiny::reactive({
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = "Getting concepts mapped to concept ids resolved by concept set expression (may take time)", value = 0)
      output <- mappedConceptSet(dataSource = dataSource,
                                 databaseIds = selectedDatabaseIds(),
                                 cohortId = targetCohortId())
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })

    getFilteredConceptIds <- shiny::reactive({
      validate(need(hasData(selectedDatabaseIds()), "No data sources chosen"))
      validate(need(hasData(targetCohortId()), "No cohort chosen"))
      validate(need(hasData(conceptSetIds()), "No concept set id chosen"))
      resolved <- getResolvedConcepts()
      mapped <- getMappedConcepts()
      output <- c()
      if (hasData(resolved)) {
        resolved <- resolved %>%
          dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
          dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
          dplyr::filter(.data$conceptSetId %in% conceptSetIds())
        output <- c(output, resolved$conceptId) %>% unique()
      }
      if (hasData(mapped)) {
        mapped <- mapped %>%
          dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
          dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
          dplyr::filter(.data$conceptSetId %in% conceptSetIds())
        output <- c(output, mapped$conceptId) %>% unique()
      }

      if (hasData(output)) {
        return(output)
      } else {
        return(NULL)
      }
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