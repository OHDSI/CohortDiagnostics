#' inclusion Rules View
inclusionRulesView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      status = "warning",
      width = "100%",
      tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = "selectedCohort")
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
              align = "left",
              shiny::radioButtons(
                inputId = "inclusionRuleTableFilters",
                label = "Inclusion Rule Events",
                choices = c("All", "Meet", "Gain", "Remain", "Total"),
                selected = "All",
                inline = TRUE
              )
            ),
            td(
              align = "right",
            )
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("inclusionRuleTable")))
    )
  )
}

#' inclusion Rules Module
#'
#'
#'
#'
inclusionRulesModule <- function(id,
                                 dataSource,
                                 cohortTable,
                                 databaseTable,
                                 selectedCohort,
                                 targetCohortId,
                                 selectedDatabaseIds) {

  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohort <- shiny::renderUI(selectedCohort())

    # Inclusion rules table ------------------
    output$inclusionRuleTable <- reactable::renderReactable(expr = {
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      table <- getInclusionRuleStats(
        dataSource = dataSource,
        cohortIds = targetCohortId(),
        databaseIds = selectedDatabaseIds()
      ) %>%
        dplyr::rename(
          Meet = .data$meetSubjects,
          Gain = .data$gainSubjects,
          Remain = .data$remainSubjects,
          Total = .data$totalSubjects
        )

      validate(need(
        (nrow(table) > 0),
        "There is no data for the selected combination."
      ))

      keyColumnFields <-
        c("cohortId", "ruleName")
      countLocation <- 1
      if (input$inclusionRuleTableFilters == "All") {
        dataColumnFields <- c("Meet", "Gain", "Remain", "Total")
      } else {
        dataColumnFields <- input$inclusionRuleTableFilters
      }

      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = targetCohortId(),
          source = "cohort",
          fields = "Persons"
        )

      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = table,
          string = dataColumnFields
        )

      showDataAsPercent <- FALSE
      ## showDataAsPercent set based on UI selection - proportion

      getDisplayTableGroupedByDatabaseId(
        data = table,
        cohort = cohortTable,
        database = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = showDataAsPercent,
        sort = TRUE
      )
    })
  })
}