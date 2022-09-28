#'
#'
#'
#'
indexEventBreakdownView <- function(id) {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Index Events",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "indexEventBreakdown.html"))
    ),
    shinydashboard::box(
      status = "warning",
      width = "100%",
      tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohort"))
      )
    ),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              shiny::radioButtons(
                inputId = ns("indexEventBreakdownTableRadioButton"),
                label = "",
                choices = c("All", "Standard concepts", "Non Standard Concepts"),
                selected = "All",
                inline = TRUE
              )
            ),
            td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
            td(
              shiny::radioButtons(
                inputId = ns("indexEventBreakdownTableFilter"),
                label = "Display",
                choices = c("Both", "Records", "Persons"),
                selected = "Persons",
                inline = TRUE
              )
            )
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("breakdownTable"))),
      csvDownloadButton(ns, "breakdownTable")
    )
  )
}

#'
#'
#'
indexEventBreakdownModule <- function(id,
                                      dataSource,
                                      selectedCohort,
                                      targetCohortId,
                                      selectedDatabaseIds) {
  ns <- shiny::NS(id)

  serverFunction <- function(input, output, session) {

    output$selectedCohort <- shiny::renderUI(selectedCohort())

    # Index event breakdown -----------
    indexEventBreakDownData <- shiny::reactive(x = {
      if (length(targetCohortId()) > 0 &&
        length(selectedDatabaseIds()) > 0) {
        data <- getIndexEventBreakdown(
          dataSource = dataSource,
          cohortIds = targetCohortId(),
          databaseIds = selectedDatabaseIds()
        )
        if (any(
          is.null(data),
          nrow(data) == 0
        )) {
          return(NULL)
        }
        if (!is.null(data)) {
          if (!"domainTable" %in% colnames(data)) {
            data$domainTable <- "Not in data"
          }
          if (!"domainField" %in% colnames(data)) {
            data$domainField <- "Not in data"
          }
          return(data)
        } else {
          return(NULL)
        }
      } else {
        return(NULL)
      }
    })

    indexEventBreakDownDataFilteredByRadioButton <-
      shiny::reactive(x = {
        data <- indexEventBreakDownData()
        if (!is.null(data) && nrow(data) > 0) {
          if (input$indexEventBreakdownTableRadioButton == "All") {
            return(data)
          } else if (input$indexEventBreakdownTableRadioButton == "Standard concepts") {
            return(data %>% dplyr::filter(.data$standardConcept == "S"))
          } else {
            return(data %>% dplyr::filter(is.na(.data$standardConcept)))
          }
        } else {
          return(NULL)
        }
      })

    output$breakdownTable <- reactable::renderReactable(expr = {
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(targetCohortId()) > 0, "No cohorts chosen chosen"))

      showDataAsPercent <- FALSE # input$indexEventBreakDownShowAsPercent
      data <- indexEventBreakDownDataFilteredByRadioButton()
     
      validate(need(
        all(!is.null(data), nrow(data) > 0),
        "There is no data for the selected combination."
      ))

      validate(need(
        nrow(data) > 0,
        "No data available for selected combination."
      ))

      data <- data %>%
        dplyr::arrange(.data$databaseId) %>%
        dplyr::select(
          .data$conceptId,
          .data$conceptName,
          .data$domainField,
          .data$databaseId,
          .data$vocabularyId,
          .data$conceptCode,
          .data$conceptCount,
          .data$subjectCount,
          .data$subjectPercent,
          .data$conceptPercent
        ) %>%
        dplyr::filter(.data$conceptId > 0) %>%
        dplyr::distinct()

      if (showDataAsPercent) {
        data <- data %>%
          dplyr::rename(
            persons = .data$subjectPercent,
            records = .data$conceptPercent
          )
      } else {
        data <- data %>%
          dplyr::rename(
            persons = .data$subjectCount,
            records = .data$conceptCount
          )
      }

      data <- data %>%
        dplyr::arrange(dplyr::desc(abs(dplyr::across(
          c("records", "persons")
        ))))

      keyColumnFields <-
        c("conceptId", "conceptName", "conceptCode", "domainField", "vocabularyId")
      if (input$indexEventBreakdownTableFilter == "Persons") {
        dataColumnFields <- c("persons")
        countLocation <- 1
      } else if (input$indexEventBreakdownTableFilter == "Records") {
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
          fields = input$indexEventBreakdownTableFilter
        )

      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = data,
          string = dataColumnFields
        )


      getDisplayTableGroupedByDatabaseId(
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
    })
  }


  return(shiny::moduleServer(id, serverFunction))
}