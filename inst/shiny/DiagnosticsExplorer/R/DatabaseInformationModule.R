databaseInformationView <- function(id) {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      width = NULL,
      title = "Execution meta-data",
      tags$p("Each entry relates to execution on a given cdm. Results are merged between executions incrementally"),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("databaseInformationTable"))),
      shiny::conditionalPanel(
        "output.databaseInformationTableIsSelected == true",
        ns = ns,
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
}

databaseInformationModule <- function(id,
                                      dataSource,
                                      selectedDatabaseIds,
                                      databaseMetadata) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {

    getDatabaseInformation <- shiny::reactive(x = {
      return(databaseMetadata %>% dplyr::filter(.data$databaseId %in% selectedDatabaseIds()))
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
        dataColumns = dataColumns,
        selection = "single"
      )
    })

    selectedDbRow <- shiny::reactive({
      reactable::getReactableState("databaseInformationTable", "selected")
    })

    output$databaseInformationTableIsSelected <- shiny::reactive({
      return(!is.null(selectedDbRow()))
    })

    shiny::outputOptions(output,
                  "databaseInformationTableIsSelected",
                  suspendWhenHidden = FALSE)

    getFilteredMetadataInformation <- shiny::reactive(x = {
      idx <- selectedDbRow()
      dbInfo <- getDatabaseInformation()[idx,]
      if (is.null(dbInfo)) {
        return(NULL)
      }
      data <- getExecutionMetadata(dataSource = dataSource,
                                   databaseId = dbInfo$databaseId)

       if (is.null(data)) {
        return(NULL)
      }

      # The meta-data data structure needs to be taken out!
      data <- data %>%
        dplyr::mutate(startTime = paste0(.data$startTime)) %>%
        dplyr::mutate(startTime = as.POSIXct(.data$startTime))

      data <- data %>% dplyr::filter(.data$startTime == dbInfo$startTime)
      return(data)
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