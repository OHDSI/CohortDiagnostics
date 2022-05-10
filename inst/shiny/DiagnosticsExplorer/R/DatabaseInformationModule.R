databaseInformationView <- function(id) {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      width = NULL,
      title = NULL,
      shiny::tabsetPanel(
        id = "metadataInformationTabsetPanel",
        shiny::tabPanel(
          title = "Data source",
          value = "datasourceTabPanel",
          tags$br(),
          htmltools::withTags(table(
            width = "100%",
            tags$tr(
              tags$td(
                align = "right",
              )
            )
          )),
          tags$br(),
          shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("databaseInformationTable")))
        ),
        shiny::tabPanel(
          title = "Meta data information",
          value = "metaDataInformationTabPanel",
          tags$br(),
          shinydashboard::box(
            title = shiny::htmlOutput(outputId = ns("metadataInfoTitle")),
            collapsible = TRUE,
            width = NULL,
            collapsed = FALSE,
            shiny::htmlOutput(outputId = ns("metadataInfoDetailsText")),
            shinydashboard::box(
              title = NULL,
              collapsible = TRUE,
              width = NULL,
              collapsed = FALSE,
              shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("packageDependencySnapShotTable")))
            ),
            shinydashboard::box(
              title = NULL,
              collapsible = TRUE,
              width = NULL,
              collapsed = FALSE,
              shiny::verbatimTextOutput(outputId = ns("argumentsAtDiagnosticsInitiationJson")),
              tags$head(
                tags$style("#argumentsAtDiagnosticsInitiationJson { max-height:400px};")
              )
            )
          )
        )
      )
    )
  )
}

databaseInformationModule <- function(id,
                                      dataSource,
                                      selectedDatabaseIds,
                                      databaseTable) {

  shiny::moduleServer(id, function(input, output, session) {

    getFilteredMetadataInformation <- shiny::reactive(x = {
      data <- getExecutionMetadata(dataSource = dataSource)
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::filter(.data$databaseId == selectedDatabaseIds())
      return(data)
    })

    getDatabaseInformation <- shiny::reactive(x = {
      return(databaseTable)
    })

    # Output: databaseInformationTable ------------------------
    output$databaseInformationTable <- reactable::renderReactable(expr = {
      data <- getDatabaseInformation()
      validate(need(
        all(!is.null(data), nrow(data) > 0),
        "No data available for selected combination."
      ))

      if (!"vocabularyVersionCdm" %in% colnames(data)) {
        data$vocabularyVersionCdm <- "Not in data"
      }
      if (!"vocabularyVersion" %in% colnames(data)) {
        data$vocabularyVersion <- "Not in data"
      }

      keyColumns <- intersect(
        colnames(data),
        c(
          "databaseId",
          "databaseName",
          "vocabularyVersionCdm",
          "vocabularyVersion",
          "description",
          "startTime",
          "runTime",
          "runTimeUnits",
          "sourceReleaseDate",
          "cdmVersion",
          "cdmReleaseDate",
          "observationPeriodMinDate",
          "observationPeriodMaxDate"
        )
      )

      dataColumns <- c(
        "personsInDatasource",
        "recordsInDatasource",
        "personDaysInDatasource"
      )

      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns
      )
    })

    output$metadataInfoTitle <- shiny::renderUI(expr = {
      data <- getFilteredMetadataInformation()

      if (!hasData(data)) {
        return(NULL)
      }
      tags$p(paste(
        "Run on ",
        data$databaseId,
        "on ",
        data$startTime,
        " for ",
        data$runTime,
        " ",
        data$runTimeUnits
      ))
    })

    output$metadataInfoDetailsText <- shiny::renderUI(expr = {
      data <- getFilteredMetadataInformation()
      if (!hasData(data)) {
        return(NULL)
      }
      tags$table(tags$tr(tags$td(
        paste(
          "Ran for ",
          data$runTime,
          data$runTimeUnits,
          "on ",
          data$currentPackage,
          "(",
          data$currentPackageVersion,
          ")"
        )
      )))
    })

    ## output: packageDependencySnapShotTable----
    output$packageDependencySnapShotTable <-
      reactable::renderReactable(expr = {
        data <- getFilteredMetadataInformation()
        if (!hasData(data)) {
          return(NULL)
        }
        data <- data %>%
          dplyr::pull(.data$packageDependencySnapShotJson)

        data <- dplyr::as_tibble(RJSONIO::fromJSON(
          content = data,
          digits = 23
        ))
        keyColumns <- colnames(data)
        getDisplayTableSimple(
          data = data,
          keyColumns = keyColumns,
          dataColumns = c(),
          pageSize = 10
        )
      })

    ## output: argumentsAtDiagnosticsInitiationJson----
    output$argumentsAtDiagnosticsInitiationJson <-
      shiny::renderText(expr = {
        data <- getFilteredMetadataInformation()
        if (!hasData(data)) {
          return(NULL)
        }
        data <- data %>%
          dplyr::pull(.data$argumentsAtDiagnosticsInitiationJson) %>%
          RJSONIO::fromJSON(digits = 23) %>%
          RJSONIO::toJSON(
            digits = 23,
            pretty = TRUE
          )
        return(data)
      })
  })
}