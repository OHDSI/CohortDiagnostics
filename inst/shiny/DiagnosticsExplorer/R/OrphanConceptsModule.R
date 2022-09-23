#' Orphan Concepts View
#'
orpahanConceptsView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Orphan Concepts",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "orphanConcepts.html"))
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
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              shiny::radioButtons(
                inputId = ns("orphanConceptsType"),
                label = "Filters",
                choices = c("All", "Standard Only", "Non Standard Only"),
                selected = "All",
                inline = TRUE
              )
            ),
            td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
            td(
              shiny::radioButtons(
                inputId = ns("orphanConceptsColumFilterType"),
                label = "Display",
                choices = c("All", "Persons", "Records"),
                selected = "All",
                inline = TRUE
              )
            )
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("orphanConceptsTable"))),
      csvDownloadButton(ns, "orphanConceptsTable")
    )
  )
}


orphanConceptsModule <- function(id,
                                 dataSource,
                                 selectedCohorts,
                                 selectedDatabaseIds,
                                 targetCohortId,
                                 selectedConceptSets,
                                 conceptSetIds) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI({ selectedCohorts() })


    # Orphan concepts table --------------------
    orphanConceptsDataReactive <- shiny::reactive(x = {
      validate(need(length(targetCohortId()) > 0, "No cohorts chosen"))
      data <- getOrphanConceptResult(
        dataSource = dataSource,
        cohortId = targetCohortId(),
        databaseIds = selectedDatabaseIds()
      )
      
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::arrange(dplyr::desc(.data$conceptCount))
      return(data)
    })
    
    # Reactive below developed for testing purposes
    # Focuses on filtering the standard vs. non-standard codes
    filteringStandardConceptsReactive <- shiny::reactive(x = {
      data <- orphanConceptsDataReactive()
      validate(need(hasData(data), "There is no data for the selected combination."))
      
      
      if (hasData(selectedConceptSets())) {
        if (!is.null(selectedConceptSets())) {
          if (length(conceptSetIds()) > 0) {
            data <- data %>%
              dplyr::filter(.data$conceptSetId %in% conceptSetIds())
          } else {
            data <- data[0,]
          }
        }
      }

      if (input$orphanConceptsType == "Standard Only") {
        data <- data %>%
          dplyr::filter(.data$standardConcept == "S")
      } else if (input$orphanConceptsType == "Non Standard Only") {
        data <- data %>%
          dplyr::filter(is.na(.data$standardConcept) |
                          (
                            all(!is.na(.data$standardConcept), .data$standardConcept != "S")
                          ))
      }
      
      return (data)
      
    })

    output$orphanConceptsTable <- reactable::renderReactable(expr = {
      
      data <- filteringStandardConceptsReactive()
      validate(need(hasData(data), "There is no data for the selected combination."))
    

      data <- data %>%
        dplyr::select(
          .data$databaseId,
          .data$cohortId,
          .data$conceptId,
          .data$conceptSubjects,
          .data$conceptCount
        ) %>%
        dplyr::group_by(
          .data$databaseId,
          .data$cohortId,
          .data$conceptId
        ) %>%
        dplyr::summarise(
          conceptSubjects = sum(.data$conceptSubjects),
          conceptCount = sum(.data$conceptCount),
          .groups = "keep"
        ) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(
          .data$databaseId,
          .data$cohortId
        ) %>%
        dplyr::inner_join(
          data %>%
            dplyr::select(
              .data$conceptId,
              .data$databaseId,
              .data$cohortId,
              .data$conceptName,
              .data$vocabularyId,
              .data$conceptCode
            ),
          by = c("databaseId", "cohortId", "conceptId")
        ) %>%
        dplyr::rename(
          persons = .data$conceptSubjects,
          records = .data$conceptCount
        ) %>%
        dplyr::arrange(dplyr::desc(abs(dplyr::across(
          c("records", "persons")
        ))))

      keyColumnFields <-
        c("conceptId", "conceptName", "vocabularyId", "conceptCode")
      if (input$orphanConceptsColumFilterType == "Persons") {
        dataColumnFields <- c("persons")
        countLocation <- 1
      } else if (input$orphanConceptsColumFilterType == "Records") {
        dataColumnFields <- c("records")
        countLocation <- 1
      } else {
        dataColumnFields <- c("persons", "records")
        countLocation <- 2
      }
      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = data$databaseId %>% unique(),
          cohortIds = data$cohortId %>% unique(),
          source = "cohort",
          fields = input$orphanConceptsColumFilterType
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
        cohort = cohort,
        databaseTable = database,
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