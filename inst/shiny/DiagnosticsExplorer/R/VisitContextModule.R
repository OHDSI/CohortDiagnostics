visitContextView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Visit Context",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "visitContext.html"))
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
      width = NULL,
      title = NULL,
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            shiny::radioButtons(
              inputId = ns("visitContextTableFilters"),
              label = "Display",
              choices = c("All", "Before", "During", "Simultaneous", "After"),
              selected = "All",
              inline = TRUE
            )
          ),
          tags$td(
            shiny::radioButtons(
              inputId = ns("visitContextPersonOrRecords"),
              label = "Display",
              choices = c("Persons", "Records"),
              selected = "Persons",
              inline = TRUE
            )
          ),
          tags$td(
            align = "right",
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("visitContextTable"))),
      csvDownloadButton(ns, "visitContextTable")
    )
  )
}


visitContextModule <- function(id,
                               dataSource,
                               selectedCohort, #this is selectedCohorts in other modules
                               selectedDatabaseIds,
                               targetCohortId,
                               cohortTable,
                               databaseTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI(selectedCohort())
    
    # Visit Context ----------------------------------------
    getVisitContextData <- shiny::reactive(x = {
      if (!hasData(selectedDatabaseIds())) {
        return(NULL)
      }
      if (all(is(dataSource, "environment"), !exists("visitContext"))) {
        return(NULL)
      }
      visitContext <-
        getVisitContextResults(
          dataSource = dataSource,
          cohortIds = targetCohortId(),
          databaseIds = selectedDatabaseIds()
        )
      if (!hasData(visitContext)) {
        return(NULL)
      }
      return(visitContext)
    })

    ## getVisitContexDataEnhanced----
    getVisitContexDataEnhanced <- shiny::reactive(x = { #spelling error here missing the t in Context
      visitContextData <- getVisitContextData() %>%
        dplyr::rename(visitContextSubject = .data$subjects)
      if (!hasData(visitContextData)) {
        return(NULL)
      }
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
            subjects = .data$cohortSubjects,
            records = .data$cohortEntries
          ) %>%
          dplyr::select(
            .data$databaseId,
            .data$cohortId,
            .data$visitConceptName,
            .data$visitContext,
            .data$subjects,
            .data$records,
            .data$visitContextSubject
          ) %>%
          dplyr::mutate(
            visitContext = dplyr::case_when(
              .data$visitContext == "During visit" ~ "During",
              .data$visitContext == "On visit start" ~ "Simultaneous",
              TRUE ~ .data$visitContext
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
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(targetCohortId()) > 0, "No cohorts chosen"))
      data <- getVisitContexDataEnhanced()
      validate(need(
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
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = data,
          string = dataColumnFields
        )
    
      getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        sort = TRUE
      )
    })
  })
}