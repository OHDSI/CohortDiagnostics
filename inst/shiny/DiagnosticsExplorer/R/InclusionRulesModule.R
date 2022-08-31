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
                inputId = ns("inclusionRuleTableFilters"),
                label = "Inclusion Rule Events",
                choices = c("All", "Meet", "Gain", "Remain"),
                selected = "All",
                inline = TRUE
              )
            ),
            tags$td(
              shiny::checkboxInput(
                inputId = ns("inclusionRulesShowAsPercent"),
                label = "Show as percent",
                value = TRUE
              )
            ),
            td(
              align = "right",
            )
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("inclusionRuleTable"))),
      csvDownloadButton(ns, "inclusionRuleTable")
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
      table <- getInclusionRuleStatsEvents(
        dataSource = dataSource,
        cohortIds = targetCohortId(),
        databaseIds = selectedDatabaseIds()
      ) 
      
      showDataAsPercent <- input$inclusionRulesShowAsPercent
      
      if (showDataAsPercent) {
        table <- table %>%
          dplyr::mutate(
            Meet = .data$meetSubjects / .data$totalSubjects,
            Gain = .data$gainSubjects / .data$totalSubjects,
            Remain = .data$remainSubjects / .data$totalSubjects,
            id = .data$ruleSequenceId
          )
      } else {
        table <- table %>%
          dplyr::mutate(
            Meet = .data$meetSubjects,
            Gain = .data$gainSubjects,
            Remain = .data$remainSubjects,
            Total = .data$totalSubjects,
            id = .data$ruleSequenceId
          )
      }
      
      table <- table %>%
        dplyr::arrange(.data$cohortId,
                       .data$databaseId,
                       .data$id)

      validate(need(
        (nrow(table) > 0),
        "There is no data for the selected combination."
      ))

      keyColumnFields <-
        c("id", "ruleName")
      countLocation <- 1
      
      if (any(!hasData(input$inclusionRuleTableFilters),
              input$inclusionRuleTableFilters == "All")) {
        dataColumnFields <- c("Meet", "Gain", "Remain")
      } else {
        dataColumnFields <- c(input$inclusionRuleTableFilters)
      }
      
      if (all(hasData(showDataAsPercent), !showDataAsPercent)) {
        dataColumnFields <- c(dataColumnFields, "Total")
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

      getDisplayTableGroupedByDatabaseId(
        data = table,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = showDataAsPercent,
        sort = FALSE
      )
    })
  })
}
