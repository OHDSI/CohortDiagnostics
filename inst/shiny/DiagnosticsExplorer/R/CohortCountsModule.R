#' getColumnMax
#'
#' @description
#' Get Max Value For String Matched Columns
#'
getColumnMax <- function(data, string) {
  if (!hasData(data)) {
    return(0)
  }
  string <- intersect(
    string,
    colnames(data)
  )
  data <- data %>%
    dplyr::select(dplyr::all_of(string)) %>%
    tidyr::pivot_longer(values_to = "value", cols = dplyr::everything()) %>%
    dplyr::filter(!is.na(.data$value)) %>%
    dplyr::pull(.data$value)

  if (!hasData(data)) {
    return(0)
  } else {
    return(max(data, na.rm = TRUE))
  }
}


#' Cohort Counts View
#' @description
#' Shiny view for cohort counts module
#' @inputId cohortCountsTableColumnFilter              Column filters
#' @outputId cohortCountsTable                         Reactable output of cohort counts for specified databases
#' @outputId inclusionRuleStats    Reactable output of inclusion rules
cohortCountsView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Cohort Counts",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "cohortCounts.html"))
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
      width = "100%",
      shiny::tagList(
        tags$table(
          tags$tr(
            tags$td(
              shiny::radioButtons(
                inputId = ns("cohortCountsTableColumnFilter"),
                label = "Display",
                choices = c("Both", "Persons", "Records"),
                selected = "Both",
                inline = TRUE
              )
            )
          )
        ),
        shinycssloaders::withSpinner(
          reactable::reactableOutput(outputId = ns("cohortCountsTable")
          )
        ),
        csvDownloadButton(ns, "cohortCountsTable"),
        shiny::conditionalPanel(
          condition = "output.cohortCountRowIsSelected == true",
          ns = ns,
          tags$h4("Inclusion Rule Statistics"),

          shiny::fluidRow(
            shiny::column(
              width = 4,
              shiny::radioButtons(
                inputId = ns("cohortCountInclusionRuleTableFilters"),
                label = "Inclusion Rule Events",
                choices = c("All", "Meet", "Gain", "Remain"),
                selected = "All",
                inline = TRUE
              )
            ),
            shiny::column(
              width = 4,
              shiny::radioButtons(
                inputId = ns("showPersonOrEvents"),
                label = "Report",
                choices = c("Persons", "Events"),
                selected = "Persons",
                inline = TRUE
              )
            ),
            shiny::column(
              width = 4,
              shiny::checkboxInput(
                inputId = ns("showAsPercent"),
                label = "Show as percent",
                value = TRUE
              )
            )
          ),
          shinycssloaders::withSpinner(
            reactable::reactableOutput(ns("inclusionRuleStats"))
          ),
          csvDownloadButton(ns, "inclusionRuleStats")
        )
      )
    )
  )
}

#' Shiny module for cohort counts
#' @description
#' Shiny module for cohort counts. Displays reactable table of cohort counts
#'
#' @requiredPackage reactable
#' @requiredPacakge shiny
#' @requiredPacakge shinycssloaders
#' @requiredPacakge shinydashboard
#' @requiredPacakge dplyr
#'
#' @param dataSource                Backend Data source (DatabaseConnection)
#' @param cohortTable               data.frame of all cohorts
#' @param databaseTable             data.frame of all databases
#' @param selectedCohorts           shiny::reactive - should return cohorts selected or NULL
#' @param selectedDatabaseIds       shiny::reactive - should return cohorts selected or NULL
#' @param cohortIds                 shiny::reactive - should return cohorts selected integers or NULL
cohortCountsModule <- function(id,
                               dataSource,
                               cohortTable,
                               databaseTable,
                               selectedCohorts,
                               selectedDatabaseIds,
                               cohortIds) {
  ns <- shiny::NS(id)

  serverFunction <- function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI(selectedCohorts())


    # Cohort Counts ----------------------
    getResults <- shiny::reactive(x = {
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
      data <- getResultsCohortCounts(
        dataSource = dataSource,
        databaseIds = selectedDatabaseIds(),
        cohortIds = cohortIds()
      )
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data %>%
        dplyr::inner_join(cohortTable %>% dplyr::select(.data$cohortName, .data$cohortId), by = "cohortId") %>%
        dplyr::arrange(.data$cohortId, .data$databaseId)

      return(data)
    })

    output$cohortCountsTable <- reactable::renderReactable(expr = {
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))

      data <- getResults()
      validate(need(hasData(data), "There is no data on any cohort"))

      data <- getResults() %>%
        dplyr::rename(
          persons = .data$cohortSubjects,
          records = .data$cohortEntries
        )

      dataColumnFields <- c("persons", "records")

      if (input$cohortCountsTableColumnFilter == "Persons") {
        dataColumnFields <- "persons"
      } else if (input$cohortCountsTableColumnFilter == "Records") {
        dataColumnFields <- "records"
      }

      keyColumnFields <- c("cohortId", "cohortName")

      countsForHeader <- NULL

      maxCountValue <-
        getColumnMax(
          data = data,
          string = dataColumnFields
        )

      displayTable <- getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        sort = TRUE,
        selection = "single"
      )
      return(displayTable)
    })

    getCohortIdOnCohortCountRowSelect <- shiny::reactive({
      idx <- reactable::getReactableState(outputId = "cohortCountsTable", "selected")
      if (!hasData(idx)) {
        return(NULL)
      } else {
        if (hasData(getResults())) {
          subset <- getResults() %>%
            dplyr::select(
              .data$cohortId
            ) %>%
            dplyr::distinct()
          subset <- subset[idx,]
          return(subset)
        } else {
          return(NULL)
        }
      }
    })

    output$cohortCountRowIsSelected <- reactive({
      return(!is.null(getCohortIdOnCohortCountRowSelect()))
    })

    outputOptions(output,
                  "cohortCountRowIsSelected",
                  suspendWhenHidden = FALSE)

    output$inclusionRuleStats <- reactable::renderReactable(expr = {
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(
        nrow(getCohortIdOnCohortCountRowSelect()) > 0,
        "No cohorts chosen"
      ))

      if (!hasData(getCohortIdOnCohortCountRowSelect())) {
        return(NULL)
      }
      if (any(
        !hasData(input$showPersonOrEvents),
        input$showPersonOrEvents == "Persons"
      )) {
        mode <- 1
      } else {
        mode <- 0
      }
      
      data <- getInclusionRuleStats(
          dataSource = dataSource,
          cohortIds = getCohortIdOnCohortCountRowSelect()$cohortId,
          databaseIds = selectedDatabaseIds(),
          mode = mode # modeId = 1 - best event, i.e. person
        )

      showDataAsPercent <- input$showAsPercent

      validate(need(
        (nrow(data) > 0),
        "There is no data for the selected combination."
      ))

      if (all(hasData(showDataAsPercent), showDataAsPercent)) {
        data <- data %>%
          dplyr::mutate(
            Meet = .data$meetSubjects / .data$totalSubjects,
            Gain = .data$gainSubjects / .data$totalSubjects,
            Remain = .data$remainSubjects / .data$totalSubjects,
            id = .data$ruleSequenceId
          )
      } else {
        data <- data %>%
          dplyr::mutate(
            Meet = .data$meetSubjects,
            Gain = .data$gainSubjects,
            Remain = .data$remainSubjects,
            Total = .data$totalSubjects,
            id = .data$ruleSequenceId
          )
      }

      data <- data %>%
        dplyr::arrange(.data$cohortId,
                       .data$databaseId,
                       .data$id)

      validate(need(
        (nrow(data) > 0),
        "There is no data for the selected combination."
      ))

      keyColumnFields <-
        c("id", "ruleName")
      countLocation <- 1

      if (any(!hasData(input$cohortCountInclusionRuleTableFilters),
              input$cohortCountInclusionRuleTableFilters == "All")) {
        dataColumnFields <- c("Meet", "Gain", "Remain")
      } else {
        dataColumnFields <- c(input$cohortCountInclusionRuleTableFilters)
      }

      if (all(hasData(showDataAsPercent), !showDataAsPercent)) {
        dataColumnFields <- c(dataColumnFields, "Total")
      }

      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = getCohortIdOnCohortCountRowSelect()$cohortId,
          source = "cohort",
          fields = "Persons"
        )

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