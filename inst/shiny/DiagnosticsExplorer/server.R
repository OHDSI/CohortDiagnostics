shiny::shinyServer(function(input, output, session) {

  # Reacive: targetCohortId
  targetCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$targetCohort])
  })

  # ReaciveVal: cohortIds
  cohortIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$cohorts_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$cohorts_open) || !is.null(input$tabs)) {
      selectedCohortIds <-
        cohort$cohortId[cohort$compoundName %in% input$cohorts]
      cohortIds(selectedCohortIds)
    }
  })

  # Reacive: comparatorCohortId
  comparatorCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$comparatorCohort])
  })

  selectedConceptSets <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$conceptSetsSelected_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$conceptSetsSelected_open) || !is.null(input$tabs)) {
      selectedConceptSets(input$conceptSetsSelected)
    }
  })

  # conceptSetIds ----
  conceptSetIds <- shiny::reactive(x = {
    conceptSetsFiltered <- conceptSets %>%
      dplyr::filter(.data$conceptSetName %in% selectedConceptSets()) %>%
      dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
      dplyr::select(.data$conceptSetId) %>%
      dplyr::pull() %>%
      unique()
    return(conceptSetsFiltered)
  })

  timeIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$timeIdChoices_open,
      input$tabs
    )
  }, handlerExpr = {
    if (exists("temporalCharacterizationTimeIdChoices") &&
      (isFALSE(input$timeIdChoices_open) ||
        !is.null(input$tabs))) {
      if (!is.null(temporalChoices)) {
        selectedTimeIds <- temporalCharacterizationTimeIdChoices %>%
          dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
          dplyr::pull(.data$timeId)
        timeIds(selectedTimeIds)
      }
    }
  })

  ## ReactiveValue: selectedDatabaseIds ----
  selectedDatabaseIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$databases_open)
  }, handlerExpr = {
    if (isFALSE(input$databases_open)) {
      selectedDatabaseIds(input$databases)
    }
  })

  shiny::observeEvent(eventExpr = {
    list(input$database_open)
  }, handlerExpr = {
    if (isFALSE(input$database_open)) {
      selectedDatabaseIds(input$database)
    }
  })

  shiny::observeEvent(eventExpr = {
    list(input$tabs)
  }, handlerExpr = {
    if (!is.null(input$tabs)) {
      if (input$tabs %in% c(
        "compareCohortCharacterization",
        "compareTemporalCharacterization",
        "temporalCharacterization",
        "databaseInformation"
      )) {
        selectedDatabaseIds(input$database)
      } else {
        selectedDatabaseIds(input$databases)
      }
    }
  })


  ## ReactiveValue: selectedTemporalTimeIds ----
  selectedTemporalTimeIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$timeIdChoices_open,
      input$timeIdChoices,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$timeIdChoices_open) ||
      !is.null(input$tabs) & !is.null(temporalCharacterizationTimeIdChoices)) {
      selectedTemporalTimeIds(
        timeIds <- temporalCharacterizationTimeIdChoices %>%
          dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
          dplyr::pull(.data$timeId) %>%
          unique() %>%
          sort()
      )
    }
  })

  cohortSubset <- shiny::reactive({
    return(cohort %>%
             dplyr::arrange(.data$cohortId))
  })

  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "targetCohort",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })

  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "cohorts",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = c(subset[1], subset[2])
    )
  })

  shiny::observe({
    if (input$tabs == "cohortCounts" |
      input$tabs == "cohortOverlap" |
      input$tabs == "incidenceRate" |
      input$tabs == "timeDistribution") {
      subset <- input$cohorts
    } else {
      subset <- input$targetCohort
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = paste0("targetCohort", input$tabs),
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })

  shiny::observe({
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = paste0("database", input$tabs),
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = selectedDatabaseIds(),
      selected = selectedDatabaseIds()
    )
  })

  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "comparatorCohort",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset[2]
    )
  })

  cohortDefinitionTableData <- shiny::reactive(x = {
    data <- cohortSubset() %>%
      dplyr::select(cohort = .data$shortName, .data$cohortId, .data$cohortName)
    return(data)
  })

  # Cohort Definition -----
  output$cohortDefinitionTable <-
    reactable::renderReactable(expr = {
      data <- cohortDefinitionTableData() %>%
        dplyr::mutate(cohortId = as.character(.data$cohortId))

      validate(need(hasData(data), "There is no data for this cohort."))
      keyColumns <- c("cohort", "cohortId", "cohortName")
      dataColumns <- c()

      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        selection = "single"
      )
    })

  ## selectedCohortDefinitionRow -----
  selectedCohortDefinitionRow <- reactive({
    idx <- reactable::getReactableState("cohortDefinitionTable", "selected")
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- cohortSubset()
      if (nrow(subset) == 0) {
        return(NULL)
      }
      row <- subset[idx[1],]
      return(row)
    }
  })

  ## selectedCohortDefinitionRow -----
  output$cohortDefinitionRowIsSelected <- reactive({
    return(!is.null(selectedCohortDefinitionRow()))
  })

  outputOptions(output,
                "cohortDefinitionRowIsSelected",
                suspendWhenHidden = FALSE
  )

  ## cohortDetailsText ----
  output$cohortDetailsText <- shiny::renderUI({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(
        style = "margin-top: 5px;",
        tags$tr(
          tags$td(tags$strong("Cohort ID: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortId)
        ),
        tags$tr(
          tags$td(tags$strong("Cohort Name: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortName)
        )
      )
    }
  })

  ## cohortDefinitionCohortCountTable ----
  output$cohortDefinitionCohortCountTable <-
    reactable::renderReactable(expr = {
      if (is.null(selectedCohortDefinitionRow())) {
        return(NULL)
      }
      data <- cohortCount
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::filter(.data$cohortId == selectedCohortDefinitionRow()$cohortId) %>%
        dplyr::filter(.data$databaseId %in% database$databaseId) %>%
        dplyr::select(
          .data$databaseId,
          .data$cohortSubjects,
          .data$cohortEntries
        ) %>%
        dplyr::rename(
          "persons" = .data$cohortSubjects,
          "events" = .data$cohortEntries
        )

      validate(need(hasData(data), "There is no data for this cohort."))

      keyColumns <- c("databaseId")
      dataColumns <- c("persons", "events")

      displayTable <- getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns
      )
      return(displayTable)
    })

  ## cohortDefinitionCirceRDetails ----
  cohortDefinitionCirceRDetails <- shiny::reactive(x = {
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Rendering human readable cohort description using CirceR (may take time)", value = 0)
    data <- selectedCohortDefinitionRow()
    if (!hasData(data)) {
      return(NULL)
    }
    details <-
      getCirceRenderedExpression(
        cohortDefinition = data$json[1] %>% RJSONIO::fromJSON(digits = 23),
        cohortName = data$cohortName[1],
        includeConceptSets = TRUE
      )
    return(details)
  })

  output$cohortDefinitionText <- shiny::renderUI(expr = {
    cohortDefinitionCirceRDetails()$cohortHtmlExpression %>%
      shiny::HTML()
  })
  ## cohortDefinitionJson ----
  output$cohortDefinitionJson <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })

  ## cohortDefinitionSql ----
  output$cohortDefinitionSql <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$sql
    }
  })

  ## cohortDefinitionConceptSetExpression ----
  cohortDefinitionConceptSetExpression <- shiny::reactive({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    }

    expression <- RJSONIO::fromJSON(row$json, digits = 23)
    if (is.null(expression)) {
      return(NULL)
    }
    expression <-
      getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = expression)

    return(expression)
  })

  output$conceptsetExpressionsInCohort <- reactable::renderReactable(expr = {
    data <- cohortDefinitionConceptSetExpression()
    if (is.null(data)) {
      return(NULL)
    }
    if (!is.null(data$conceptSetExpression) &&
      nrow(data$conceptSetExpression) > 0) {
      data <- data$conceptSetExpression %>%
        dplyr::select(.data$id, .data$name)
    } else {
      return(NULL)
    }

    validate(need(
      all(
        !is.null(data),
        nrow(data) > 0
      ),
      "There is no data for this cohort."
    ))

    keyColumns <- c("id", "name")
    dataColumns <- c()
    getDisplayTableSimple(
      data = data,
      keyColumns = keyColumns,
      dataColumns = dataColumns,
      selection = "single"
    )
  })

  ### cohortDefinitionConceptSetExpressionSelected ----
  cohortDefinitionConceptSetExpressionSelected <- shiny::reactive(x = {
    idx <- reactable::getReactableState("conceptsetExpressionsInCohort", "selected")
    if (length(idx) == 0 || is.null(idx)) {
      return(NULL)
    }
    if (hasData(cohortDefinitionConceptSetExpression()$conceptSetExpression)) {
      data <-
        cohortDefinitionConceptSetExpression()$conceptSetExpression[idx,]
      if (!is.null(data)) {
        return(data)
      } else {
        return(NULL)
      }
    }
  })

  output$cohortDefinitionConceptSetExpressionRowIsSelected <- shiny::reactive(x = {
    return(!is.null(cohortDefinitionConceptSetExpressionSelected()))
  })

  shiny::outputOptions(
    x = output,
    name = "cohortDefinitionConceptSetExpressionRowIsSelected",
    suspendWhenHidden = FALSE
  )

  output$isDataSourceEnvironment <- shiny::reactive(x = {
    return(is(dataSource, "environment"))
  })
  shiny::outputOptions(
    x = output,
    name = "isDataSourceEnvironment",
    suspendWhenHidden = FALSE
  )

  ### cohortDefinitionConceptSetDetails ----
  cohortDefinitionConceptSetDetails <- shiny::reactive(x = {
    if (is.null(cohortDefinitionConceptSetExpressionSelected())) {
      return(NULL)
    }
    data <-
      cohortDefinitionConceptSetExpression()$conceptSetExpressionDetails
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$id == cohortDefinitionConceptSetExpressionSelected()$id)
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$isExcluded,
        .data$includeDescendants,
        .data$includeMapped,
        .data$standardConcept,
        .data$invalidReason,
        .data$conceptCode,
        .data$domainId,
        .data$vocabularyId,
        .data$conceptClassId
      )
    return(data)
  })

  output$cohortDefinitionConceptSetDetailsTable <-
    reactable::renderReactable(expr = {
      data <- cohortDefinitionConceptSetDetails()
      validate(need(
        all(
          !is.null(data),
          nrow(data) > 0
        ),
        "There is no data for this cohort."
      ))
      if (is.null(cohortDefinitionConceptSetDetails())) {
        return(NULL)
      }

      data <- data %>%
        dplyr::rename(
          exclude = .data$isExcluded,
          descendants = .data$includeDescendants,
          mapped = .data$includeMapped,
          invalid = .data$invalidReason
        )
      validate(need(
        all(
          !is.null(data),
          nrow(data) > 0
        ),
        "There is no data for this cohort."
      ))

      keyColumns <- c(
        "conceptId",
        "conceptName",
        "exclude",
        "descendants",
        "mapped",
        "standardConcept",
        "invalid",
        "conceptCode",
        "domainId",
        "vocabularyId",
        "conceptClassId"
      )

      dataColumns <- c()
      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns
      )
    })

  getDatabaseIdInCohortConceptSet <- shiny::reactive({
    return(database$databaseId[database$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema])
  })

  ## Cohort Concept Set
  ### getSubjectAndRecordCountForCohortConceptSet ----
  getSubjectAndRecordCountForCohortConceptSet <- shiny::reactive(x = {
    row <- selectedCohortDefinitionRow()

    if (is.null(row) || length(getDatabaseIdInCohortConceptSet()) == 0) {
      return(NULL)
    } else {
      data <- cohortCount %>%
        dplyr::filter(.data$cohortId == row$cohortId) %>%
        dplyr::filter(.data$databaseId == getDatabaseIdInCohortConceptSet()) %>%
        dplyr::select(.data$cohortSubjects, .data$cohortEntries)

      if (nrow(data) == 0) {
        return(NULL)
      } else {
        return(data)
      }
    }
  })

  ### subjectCountInCohortConceptSet ----
  output$subjectCountInCohortConceptSet <- shiny::renderUI({
    row <- getSubjectAndRecordCountForCohortConceptSet()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(
        tags$tr(
          tags$td("Subjects: "),
          tags$td(scales::comma(row$cohortSubjects, accuracy = 1))
        )
      )
    }
  })

  ### recordCountInCohortConceptSet ----
  output$recordCountInCohortConceptSet <- shiny::renderUI({
    row <- getSubjectAndRecordCountForCohortConceptSet()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(
        tags$tr(
          tags$td("Records: "),
          tags$td(scales::comma(row$cohortEntries, accuracy = 1))
        )
      )
    }
  })

  ### getCohortDefinitionResolvedConceptsReactive ------
  getCohortDefinitionResolvedConceptsReactive <-
    shiny::reactive(x = {
      selectedCohortDefinition <- selectedCohortDefinitionRow()
      if (is.null(selectedCohortDefinition)) {
        return(NULL)
      }
      selectedConceptSet <- cohortDefinitionConceptSetExpressionSelected()
      if (is.null(selectedConceptSet)) {
        return(NULL)
      }

      if (!hasData(input$databaseOrVocabularySchema)) {
        return(NULL)
      }
      databaseIdToFilter <- database %>%
        dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
        dplyr::pull(.data$databaseId)

      data <-
        resolvedConceptSet(
          dataSource = dataSource,
          databaseIds = databaseIdToFilter,
          cohortId = selectedCohortDefinition$cohortId,
          conceptSetId = selectedConceptSet$id
        )
      if (!hasData(data)) {
        return(NULL)
      }
      conceptCount <- getCountForConceptIdInCohortReactive()
      data <- data %>%
        dplyr::left_join(conceptCount,
                         by = c("databaseId", "conceptId")
        ) %>%
        dplyr::rename(
          "subjects" = .data$conceptSubjects,
          "count" = .data$conceptCount
        ) %>%
        dplyr::arrange(dplyr::desc(.data$count))
      return(data)
    })

  ### getCohortDefinitionResolvedConceptsReactiveFiltered ----
  getCohortDefinitionResolvedConceptsReactiveFiltered <-
    shiny::reactive({
      data <- getCohortDefinitionResolvedConceptsReactive()
      if (!hasData(data)) {
        return(NULL)
      }
      if (input$withRecordCount) {
        data <- data %>%
          dplyr::filter(!is.na(.data$subjects))
      }
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })

  output$cohortDefinitionResolvedConceptsTable <-
    reactable::renderReactable(expr = {
      if (input$conceptSetsType != "Resolved") {
        return(NULL)
      }

      validate(need(
        length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
        "Please select concept set"
      ))
      data <- getCohortDefinitionResolvedConceptsReactiveFiltered()

      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))
      keyColumns <- c(
        "conceptId",
        "conceptName",
        "domainId",
        "vocabularyId",
        "conceptClassId",
        "standardConcept",
        "conceptCode"
      )
      dataColumns <- c(
        "subjects",
        "count"
      )
      displayTable <- getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        selection = "single"
      )
      return(displayTable)
    })

  ### getMappedConceptsForSelectedConceptId -----
  getMappedConceptsForSelectedConceptId <- reactive({
    idx <-
      reactable::getReactableState("cohortDefinitionResolvedConceptsTable", "selected")
    if (!hasData(getCohortDefinitionResolvedConceptsReactiveFiltered())) {
      return(NULL)
    }
    if (is.null(idx)) {
      return(NULL)
    }
    subset <-
      getCohortDefinitionResolvedConceptsReactiveFiltered()[idx,]

    mappedStandard <-
      getMappedStandardConcepts(
        dataSource = dataSource,
        conceptIds = subset$conceptId
      )
    mappedSource <- getMappedSourceConcepts(
      dataSource = dataSource,
      conceptIds = subset$conceptId
    )
    mapped <- dplyr::bind_rows(
      mappedStandard,
      mappedSource
    ) %>%
      dplyr::filter(!.data$conceptId == subset$conceptId)

    databaseIdToFilter <- database %>%
      dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
      dplyr::pull(.data$databaseId)
    if (!hasData(databaseIdToFilter)) {
      return(NULL)
    }

    cohortIdSelected <- selectedCohortDefinitionRow() %>%
      dplyr::pull(.data$cohortId)
    if (is.null(row)) {
      return(NULL)
    }

    conceptCount <- getCountForConceptIdInCohortReactive()
    conceptCount <- conceptCount %>%
      dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
      dplyr::select(-.data$databaseId)

    output <- mapped %>%
      dplyr::left_join(conceptCount,
                       by = c("conceptId")
      ) %>%
      dplyr::select(-.data$searchedConceptId) %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(output)
  })

  output$cohortDefinitionResolvedRowIsSelected <-
    shiny::reactive(x = {
      return(!is.null(
        reactable::getReactableState("cohortDefinitionResolvedConceptsTable", "selected")
      ))
    })

  shiny::outputOptions(
    x = output,
    name = "cohortDefinitionResolvedRowIsSelected",
    suspendWhenHidden = FALSE
  )

  ### Output: cohortDefinitionResolvedTableSelectedConceptIdMappedConcepts ----
  output$cohortDefinitionResolvedTableSelectedConceptIdMappedConcepts <-
    reactable::renderReactable(expr = {
      data <- getMappedConceptsForSelectedConceptId()
      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))

      keyColumns <- c(
        "conceptId",
        "conceptName",
        "vocabularyId",
        "conceptCode"
      )
      dataColumns <- c(
        "conceptSubjects",
        "conceptCount"
      )

      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns
      )
    })

  ### getCohortDefinitionOrphanConceptsReactive ----
  getCohortDefinitionOrphanConceptsReactive <- shiny::reactive(x = {
    selectedCohortDefinition <- selectedCohortDefinitionRow()
    if (is.null(selectedCohortDefinition)) {
      return(NULL)
    }
    selectedConceptSet <-
      cohortDefinitionConceptSetExpressionSelected()
    if (is.null(selectedConceptSet)) {
      return(NULL)
    }

    if (!hasData(input$databaseOrVocabularySchema)) {
      return(NULL)
    }
    databaseIdToFilter <- database %>%
      dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
      dplyr::pull(.data$databaseId)

    orphanConcepts <- getOrphanConceptResult(
      dataSource = dataSource,
      databaseIds = database$databaseId,
      cohortId = selectedCohortDefinition$cohortId,
      conceptSetId = selectedConceptSet$id
    )
    if (!hasData(orphanConcepts)) {
      return(NULL)
    }

    resolvedConcepts <-
      getCohortDefinitionResolvedConceptsReactive()
    conceptIdsThatAreNotOrphan <-
      resolvedConcepts$conceptId %>% unique()

    mappedStandard <-
      getMappedStandardConcepts(
        dataSource = dataSource,
        conceptIds = conceptIdsThatAreNotOrphan
      )
    mappedSource <- getMappedSourceConcepts(
      dataSource = dataSource,
      conceptIds = conceptIdsThatAreNotOrphan
    )
    mapped <- dplyr::bind_rows(
      mappedStandard,
      mappedSource
    )
    conceptIdsThatAreNotOrphan <- c(
      conceptIdsThatAreNotOrphan,
      mapped$conceptId
    ) %>%
      unique()

    output <- orphanConcepts %>%
      dplyr::filter(!.data$conceptId %in% c(conceptIdsThatAreNotOrphan)) %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount)) %>%
      dplyr::rename(
        "persons" = .data$conceptSubjects,
        "records" = .data$conceptCount
      )

    if (!hasData(output)) {
      return(NULL)
    }
    return(output)
  })

  output$cohortDefinitionOrphanConceptTable <-
    reactable::renderReactable(expr = {
      if (input$conceptSetsType != "Orphan concepts") {
        return(NULL)
      }
      validate(need(
        hasData(input$databaseOrVocabularySchema),
        "Please select a data source"
      ))
      databaseIdToFilter <- database %>%
        dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
        dplyr::pull(.data$databaseId)

      data <- getCohortDefinitionOrphanConceptsReactive()
      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))

      if (input$withRecordCount) {
        data <- data %>%
          dplyr::filter(!is.na(.data$persons))
      }
      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))
      data <- data %>%
        dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
        dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
        dplyr::rename(
          "subjects" = .data$persons,
          "count" = .data$records,
          "standard" = .data$standardConcept
        )
      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))
      keyColumns <-
        c(
          "conceptId",
          "conceptName",
          "vocabularyId",
          "conceptCode",
          "standard"
        )
      dataColumns <- c(
        "subjects",
        "count"
      )

      displayTable <- getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns
      )
      return(displayTable)
    })

  #
  #
  #   ### resolveMappedConceptSetFromVocabularyDatabaseSchemaReactive ----
  #   resolveMappedConceptSetFromVocabularyDatabaseSchemaReactive <-
  #     shiny::reactive(x = {
  #       data <- NULL
  #       row <- selectedCohortDefinitionRow()
  #       if (is.null(row)) {
  #         return(NULL)
  #       }
  #       outputResolved <- list()
  #       outputMapped <- list()
  #       for (i in (1:length(vocabularyDatabaseSchemas))) {
  #         vocabularyDatabaseSchema <- vocabularyDatabaseSchemas[[i]]
  #         output <-
  #           resolveMappedConceptSetFromVocabularyDatabaseSchema(
  #             dataSource = dataSource,
  #             conceptSets = conceptSets %>%
  #               dplyr::filter(cohortId == selectedCohortDefinitionRow()$cohortId),
  #             vocabularyDatabaseSchema = vocabularyDatabaseSchema
  #           )
  #         outputResolved <- output$resolved
  #         outputMapped <- output$mapped
  #         outputResolved$vocabularyDatabaseSchema <-
  #           vocabularyDatabaseSchema
  #         outputMapped$vocabularyDatabaseSchema <-
  #           vocabularyDatabaseSchema
  #       }
  #       outputResolved <- dplyr::bind_rows(outputResolved)
  #       outputMapped <- dplyr::bind_rows(outputMapped)
  #       return(list(resolved = outputResolved, mapped = outputMapped))
  #     })

  ### getCountForConceptIdInCohortReactive ----
  getCountForConceptIdInCohortReactive <-
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      }
      data <- getCountForConceptIdInCohort(
        dataSource = dataSource,
        databaseIds = database$databaseId,
        cohortId = row$cohortId
      )
      return(data)
    })

  ## cohortConceptsetExpressionJson ----
  output$cohortConceptsetExpressionJson <- shiny::renderText({
    if (is.null(cohortDefinitionConceptSetExpressionSelected())) {
      return(NULL)
    }
    json <- cohortDefinitionConceptSetExpressionSelected()$json
    return(json)
  })

  ### getResolvedConceptsReactive ----
  getResolvedConceptsReactive <-
    shiny::reactive(x = {
      output <-
        resolvedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortId = targetCohortId()
        )
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })

  ### getMappedConceptsReactive ----
  getMappedConceptsReactive <-
    shiny::reactive(x = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = "Getting concepts mapped to concept ids resolved by concept set expression (may take time)", value = 0)
      output <-
        mappedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortId = targetCohortId()
        )
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })

  ### getResolvedAndMappedConceptIdsForFilters ----
  getResolvedAndMappedConceptIdsForFilters <- shiny::reactive({
    validate(need(hasData(selectedDatabaseIds()), "No data sources chosen"))
    validate(need(hasData(targetCohortId()), "No cohort chosen"))
    validate(need(hasData(conceptSetIds()), "No concept set id chosen"))
    resolved <- getResolvedConceptsReactive()
    mapped <- getMappedConceptsReactive()
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


  ## Export all cohort details ----
  output$exportAllCohortDetails <- downloadHandler(
    filename = function() {
      paste("ExportDetails", "zip", sep = ".")
    },
    content = function(file) {
      shiny::withProgress(
        message = "Export is in progress",
      {
        exportCohortDetailsAsZip(
          dataSource = dataSource,
          cohort = cohort,
          zipFile = file
        )
      },
        detail = "Please Wait"
      )
    },
    contentType = "application/zip"
  )

  # Cohort Counts ----------------------
  getResultsCohortCountReactive <- shiny::reactive(x = {
    if (!input$tabs == "cohortCounts") {
      return(NULL)
    }
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
      addShortName(cohort) %>%
      dplyr::arrange(.data$shortName, .data$databaseId)
    return(data)
  })

  output$cohortCountsTable <- reactable::renderReactable(expr = {
    validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))

    data <- getResultsCohortCountReactive()
    validate(need(hasData(data), "There is no data on any cohort"))

    data <- getResultsCohortCountReactive() %>%
      dplyr::rename(cohort = .data$shortName) %>%
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

    keyColumnFields <- c("cohortId", "cohort")

    countsForHeader <- NULL

    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(
        data = data,
        string = dataColumnFields
      )
    displayTable <- getDisplayTableGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
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

  getCohortIdOnCohortCountRowSelect <- reactive({
    idx <- reactable::getReactableState("cohortCountsTable", "selected")
    if (is.null(idx)) {
      return(NULL)
    } else {
      if (hasData(getResultsCohortCountReactive())) {
        subset <- getResultsCohortCountReactive() %>%
          dplyr::select(
            .data$cohortId,
            .data$shortName
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
                suspendWhenHidden = FALSE
  )

  output$InclusionRuleStatForCohortSeletedTable <- reactable::renderReactable(expr = {
    validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
    validate(need(
      nrow(getCohortIdOnCohortCountRowSelect()) > 0,
      "No cohorts chosen"
    ))

    if (!hasData(getCohortIdOnCohortCountRowSelect())) {
      return(NULL)
    }

    data <- getInclusionRuleStats(
      dataSource = dataSource,
      cohortIds = getCohortIdOnCohortCountRowSelect()$cohortId,
      databaseIds = selectedDatabaseIds()
    ) %>% dplyr::rename(
      Meet = .data$meetSubjects,
      Gain = .data$gainSubjects,
      Remain = .data$remainSubjects,
      Total = .data$totalSubjects
    )

    countLocation <- 1
    keyColumnFields <-
      c("cohortId", "ruleName")
    dataColumnFields <- c("Meet", "Gain", "Remain", "Total")

    validate(need(
      (nrow(data) > 0),
      "There is no data for the selected combination."
    ))


    countsForHeader <- NULL

    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(
        data = data,
        string = dataColumnFields
      )

    getDisplayTableGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = 1,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      sort = TRUE,
      selection = "single"
    )
  })

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

  output$orphanConceptsTable <- reactable::renderReactable(expr = {
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
                          !is.na(.data$standardConcept) && .data$standardConcept != "S"
                        ))
    }

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
      database = database,
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

  # Inclusion rules table ------------------
  output$inclusionRuleTable <- reactable::renderReactable(expr = {
    if (!input$tabs %in% c("inclusionRuleStats")) {
      return(NULL)
    }
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
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showDataAsPercent = showDataAsPercent,
      sort = TRUE
    )
  })

  # Index event breakdown -----------

  indexEventBreakDownData <- shiny::reactive(x = {
    if (!input$tabs %in% c("indexEventBreakdown")) {
      return(NULL)
    }
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

    showDataAsPercent <- input$indexEventBreakDownShowAsPercent
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
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showDataAsPercent = showDataAsPercent,
      sort = TRUE
    )
  })

  # Characterization (Shared across) -------------------------------------------------
  ## Reactive objects ----
  ### getConceptSetNameForFilter ----
  getConceptSetNameForFilter <- shiny::reactive(x = {
    if (!hasData(targetCohortId()) || !hasData(selectedDatabaseIds())) {
      return(NULL)
    }

    jsonExpression <- cohortSubset() %>%
      dplyr::filter(.data$cohortId == targetCohortId()) %>%
      dplyr::select(.data$json)
    jsonExpression <-
      RJSONIO::fromJSON(jsonExpression$json, digits = 23)
    expression <-
      getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = jsonExpression)
    if (is.null(expression)) {
      return(NULL)
    }

    expression <- expression$conceptSetExpression %>%
      dplyr::select(.data$name)
    return(expression)
  })


  ## characterizationOutputForCharacterizationMenu ----
  characterizationOutputForCharacterizationMenu <-
    shiny::reactive(x = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Retrieving characterization output for cohort id ",
          targetCohortId(),
          " cohorts and ",
          length(selectedDatabaseIds()),
          " data sources."
        ),
        value = 0
      )
      data <- getCharacterizationOutput(
        dataSource = dataSource,
        cohortIds = targetCohortId(),
        databaseIds = selectedDatabaseIds(),
        temporalCovariateValueDist = FALSE
      )
      return(data)
    })

  ## characterizationOutputForTemporalCharacterizationMenu ----
  characterizationOutputForTemporalCharacterizationMenu <-
    shiny::reactive(x = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Retrieving characterization output for target cohort id ",
          targetCohortId(),
          " from ",
          input$database,
          "."
        ),
        value = 0
      )

      if (input$database %in% c(selectedDatabaseIds())) {
        data <- characterizationOutputForCharacterizationMenu()
        if (hasData(data$covariateValue)) {
          data$covariateValue <- data$covariateValue %>%
            dplyr::filter(.data$databaseId %in% c(input$database))
        }
        if (hasData(data$covariateValueDist)) {
          data$covariateValueDist <- data$covariateValueDist %>%
            dplyr::filter(.data$databaseId %in% c(input$database))
        }
      } else {
        data <- getCharacterizationOutput(
          dataSource = dataSource,
          cohortIds = targetCohortId(),
          databaseIds = input$database,
          temporalCovariateValueDist = FALSE
        )
      }
      return(data)
    })

  ## characterizationOutputForCompareCharacterizationMenu ----
  characterizationOutputForCompareCharacterizationMenu <-
    shiny::reactive(x = {
      dataTarget <-
        characterizationOutputForTemporalCharacterizationMenu()
      if (!hasData(dataTarget)) {
        return(NULL)
      }

      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Retrieving characterization output for comparator cohort id ",
          comparatorCohortId(),
          " from ",
          input$database,
          "."
        ),
        value = 0
      )
      dataComparator <- getCharacterizationOutput(
        dataSource = dataSource,
        cohortIds = c(comparatorCohortId()),
        databaseIds = input$database,
        temporalCovariateValueDist = FALSE
      )
      if (!hasData(dataComparator)) {
        return(NULL)
      }
      data <- NULL
      data$covariateValue <-
        dplyr::bind_rows(
          dataTarget$covariateValue,
          dataComparator$covariateValue
        )
      if (!hasData(data$covariateValue)) {
        data$covariateValue <- NULL
      }
      data$covariateValueDist <-
        dplyr::bind_rows(
          dataTarget$covariateValueDist,
          dataComparator$covariateValueDist
        )
      if (!hasData(data$covariateValueDist)) {
        data$covariateValueDist <- NULL
      }
      return(data)
    })

  ## characterizationOutputForCompareTemporalCharacterizationMenu ----
  characterizationOutputForCompareTemporalCharacterizationMenu <-
    shiny::reactive(x = {
      data <- characterizationOutputForCompareCharacterizationMenu()
      return(data)
    })

  # Cohort Characterization -------------------------------------------------
  ## ReactiveVal: characterizationAnalysisNameFilter ----
  characterizationAnalysisNameFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$characterizationAnalysisNameFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$characterizationAnalysisNameFilter_open) ||
      !is.null(input$tabs)) {
      characterizationAnalysisNameFilter(input$characterizationAnalysisNameFilter)
    }
  })
  #### characterizationAnalysisNameFilter ----
  shiny::observe({
    characterizationAnalysisOptionsUniverse <- NULL
    charcterizationAnalysisOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      characterizationAnalysisOptionsUniverse <- analysisNameOptions
      charcterizationAnalysisOptionsSelected <- temporalAnalysisRef %>%
        dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
        dplyr::pull(.data$analysisName) %>%
        unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = characterizationAnalysisOptionsUniverse,
      selected = charcterizationAnalysisOptionsSelected
    )
  })

  ## ReactiveVal: characterizationDomainIdFilter ----
  characterizationDomainIdFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$characterizationDomainIdFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$characterizationDomainIdFilter_open) ||
      !is.null(input$tabs)) {
      characterizationDomainIdFilter(input$characterizationDomainIdFilter)
    }
  })

  ### characterizationDomainNameFilter ----
  shiny::observe({
    characterizationDomainOptionsUniverse <- NULL
    charcterizationDomainOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      characterizationDomainOptionsUniverse <- domainIdOptions
      charcterizationDomainOptionsSelected <- temporalAnalysisRef %>%
        dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
        dplyr::pull(.data$domainId) %>%
        unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationDomainIdFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = characterizationDomainOptionsUniverse,
      selected = charcterizationDomainOptionsSelected
    )
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationDomainIdFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = characterizationDomainOptionsUniverse,
      selected = charcterizationDomainOptionsSelected
    )
  })

  shiny::observe({
    subset <- getConceptSetNameForFilter()$name %>%
      sort() %>%
      unique()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "conceptSetsSelected",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })

  ## cohortCharacterizationDataFiltered ----
  cohortCharacterizationDataFiltered <- shiny::reactive(x = {
    if (!input$tabs %in% c("cohortCharacterization")) {
      return(NULL)
    }
    validate(need(length(selectedDatabaseIds()) > 0, "Atleast one data source must be selected"))
    validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))

    data <-
      characterizationOutputForCharacterizationMenu()
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data$covariateValue
    if (!hasData(data)) {
      return(NULL)
    }

    data <- data %>%
      dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
      dplyr::filter(.data$timeId %in% c(characterizationTimeIdChoices$timeId %>% unique())) %>%
      dplyr::filter(.data$cohortId %in% c(targetCohortId())) %>%
      dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds()))

    if (input$charType == "Raw") {
      if (input$characterizationProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y")
      } else if (input$characterizationProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "N")
      }
    }

    if (input$characterizationProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "Y")
    } else if (input$characterizationProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "N")
    }

    data <- data %>%
      dplyr::filter(.data$analysisName %in% characterizationAnalysisNameFilter())

    data <- data %>%
      dplyr::filter(.data$domainId %in% characterizationDomainIdFilter())

    if (hasData(selectedConceptSets())) {
      if (hasData(getResolvedAndMappedConceptIdsForFilters())) {
        data <- data %>%
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      }
    }
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })

  ## cohortCharacterizationPrettyTable ----
  cohortCharacterizationPrettyTable <- shiny::reactive(x = {
    if (!input$tabs %in% c("cohortCharacterization")) {
      return(NULL)
    }
    validate(need(length(selectedDatabaseIds()) > 0, "Atleast one data source must be selected"))
    validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
    data <-
      characterizationOutputForCharacterizationMenu()
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data$covariateValue
    if (!hasData(data)) {
      return(NULL)
    }

    data <- data %>%
      dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
      dplyr::filter(.data$timeId %in% c(characterizationTimeIdChoices$timeId %>% unique(), NA)) %>%
      dplyr::filter(.data$cohortId %in% c(targetCohortId())) %>%
      dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds()))
    if (!hasData(data)) {
      return(NULL)
    }

    showDataAsPercent <-
      TRUE ## showDataAsPercent set based on UI selection - proportion)

    if (showDataAsPercent) {
      data <- data %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$mean
        ) %>%
        dplyr::rename(sumValue = .data$mean)
    } else {
      data <- data %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$sumValue
        )
    }

    table <- data %>%
      prepareTable1(
        prettyTable1Specifications = prettyTable1Specifications,
        cohort = cohort
      )
    if (!hasData(table)) {
      return(NULL)
    }
    keyColumnFields <- c("characteristic")
    dataColumnFields <- intersect(
      x = colnames(table),
      y = cohort$shortName
    )


    countLocation <- 1
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
    displayTable <- getDisplayTableGroupedByDatabaseId(
      data = table,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showDataAsPercent = showDataAsPercent,
      sort = FALSE,
      pageSize = 100
    )
    return(displayTable)
  })

  ## cohortCharacterizationRawTable ----
  cohortCharacterizationRawTable <- shiny::reactive(x = {
    data <- cohortCharacterizationDataFiltered()
    if (!hasData(data)) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = "Post processing: Rendering table",
      value = 0
    )
    data <- data %>%
      dplyr::select(
        .data$covariateName,
        .data$analysisName,
        .data$startDay,
        .data$endDay,
        .data$conceptId,
        .data$mean,
        .data$sd,
        .data$cohortId,
        .data$databaseId,
        .data$temporalChoices
      )

    keyColumnFields <-
      c("covariateName", "analysisName", "temporalChoices", "conceptId")

    showDataAsPercent <- FALSE
    if (input$characterizationColumnFilters == "Mean and Standard Deviation") {
      dataColumnFields <- c("mean", "sd")
    } else {
      dataColumnFields <- c("mean")
      if (input$characterizationProportionOrContinuous == "Proportion") {
        showDataAsPercent <- TRUE
      }
    }
    countLocation <- 1

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
        data = data,
        string = dataColumnFields
      )

    getDisplayTableGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showDataAsPercent = showDataAsPercent,
      sort = TRUE,
      pageSize = 100
    )
  })

  ## Output: characterizationTable ----
  output$characterizationTable <- reactable::renderReactable(expr = {
    if (input$charType == "Pretty") {
      data <- cohortCharacterizationPrettyTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    } else {
      data <- cohortCharacterizationRawTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    }
  })

  # Temporal characterization ------------
  ## ReactiveVal: temporalCharacterizationAnalysisNameFilter ----
  temporalCharacterizationAnalysisNameFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$temporalCharacterizationAnalysisNameFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$temporalCharacterizationAnalysisNameFilter_open) ||
      !is.null(input$tabs)) {
      temporalCharacterizationAnalysisNameFilter(input$temporalCharacterizationAnalysisNameFilter)
    }
  })
  ### temporalCharacterizationAnalysisNameFilter ----
  shiny::observe({
    temporalCharacterizationAnalysisOptionsUniverse <- NULL
    temporalCharcterizationAnalysisOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      temporalCharacterizationAnalysisOptionsUniverse <-
        analysisNameOptions
      temporalCharcterizationAnalysisOptionsSelected <-
        temporalAnalysisRef %>%
          dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
          dplyr::pull(.data$analysisName) %>%
          unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCharacterizationAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = temporalCharacterizationAnalysisOptionsUniverse,
      selected = temporalCharcterizationAnalysisOptionsSelected
    )
  })

  ## ReactiveVal: temporalCharacterizationDomainIdFilter ----
  temporalcharacterizationDomainIdFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$temporalcharacterizationDomainIdFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$temporalcharacterizationDomainIdFilter_open) ||
      !is.null(input$tabs)) {
      temporalcharacterizationDomainIdFilter(input$temporalcharacterizationDomainIdFilter)
    }
  })

  ### temporalcharacterizationDomainIdFilter ----
  shiny::observe({
    temporalCharacterizationDomainOptionsUniverse <- NULL
    temporalCharcterizationDomainOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      temporalCharacterizationDomainOptionsUniverse <-
        domainIdOptions
      temporalCharcterizationDomainOptionsSelected <-
        temporalAnalysisRef %>%
          dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
          dplyr::pull(.data$domainId) %>%
          unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalcharacterizationDomainIdFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = temporalCharacterizationDomainOptionsUniverse,
      selected = temporalCharcterizationDomainOptionsSelected
    )
  })

  ## temporalCohortCharacterizationDataFiltered ------------
  temporalCohortCharacterizationDataFiltered <- shiny::reactive({
    if (!input$tabs %in% c("temporalCharacterization")) {
      return(NULL)
    }
    validate(need(length(input$database) == 1, "One data source must be selected"))
    validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
    if (!hasData(selectedTemporalTimeIds())) {
      return(NULL)
    }
    data <-
      characterizationOutputForCharacterizationMenu()
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data$covariateValue
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
      dplyr::filter(.data$timeId %in% selectedTemporalTimeIds()) %>%
      dplyr::filter(.data$cohortId %in% c(targetCohortId())) %>%
      dplyr::filter(.data$databaseId %in% c(input$database))

    if (input$temporalProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "Y")
    } else if (input$temporalProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "N")
    }

    data <- data %>%
      dplyr::filter(.data$analysisName %in% temporalCharacterizationAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% temporalcharacterizationDomainIdFilter())

    if (hasData(selectedConceptSets())) {
      if (hasData(getResolvedAndMappedConceptIdsForFilters())) {
        data <- data %>%
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      }
    }
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })

  ## temporalCharacterizationRawTable ----
  temporalCharacterizationRawTable <- shiny::reactive(x = {
    data <- temporalCohortCharacterizationDataFiltered()
    validate(need(
      hasData(data),
      "No temporal characterization data"
    ))
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = "Post processing: Rendering table",
      value = 0
    )

    temporalChoices <- temporalCharacterizationTimeIdChoices %>%
      dplyr::filter(.data$timeId %in% c(data$timeId %>% unique())) %>%
      dplyr::pull(.data$temporalChoices) %>%
      unique()

    keyColumns <- c("covariateName", "analysisName", "conceptId")
    data <- data %>%
      dplyr::select(
        .data$covariateName,
        .data$analysisName,
        .data$temporalChoices,
        .data$conceptId,
        .data$mean,
        .data$sd
      ) %>%
      tidyr::pivot_wider(
        id_cols = dplyr::all_of(keyColumns),
        names_from = "temporalChoices",
        values_from = "mean",
        names_sep = "_"
      ) %>%
      dplyr::relocate(dplyr::all_of(c(keyColumns, temporalChoices))) %>%
      dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with("T ("))))

    if (any(stringr::str_detect(
      string = colnames(data),
      pattern = stringr::fixed("T (0")
    ))) {
      data <- data %>%
        dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with("T (0"))))
    }

    dataColumns <- c(temporalChoices)

    showDataAsPercent <- FALSE
    if (input$temporalProportionOrContinuous == "Proportion") {
      showDataAsPercent <- TRUE
    }

    getDisplayTableSimple(
      data = data,
      keyColumns = keyColumns,
      dataColumns = dataColumns,
      showDataAsPercent = showDataAsPercent,
      pageSize = 100
    )
  })

  ## Output: temporalCharacterizationTable ------------------------
  output$temporalCharacterizationTable <-
    reactable::renderReactable(expr = {
      temporalCharacterizationRawTable()
    })

  # Compare cohort characterization --------------------------------------------
  ## ReactiveVal: compareCohortCharacterizationAnalysisNameFilter ----
  compareCohortCharacterizationAnalysisNameFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$compareCohortCharacterizationAnalysisNameFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$compareCohortCharacterizationAnalysisNameFilter_open) ||
      !is.null(input$tabs)) {
      compareCohortCharacterizationAnalysisNameFilter(input$compareCohortCharacterizationAnalysisNameFilter)
    }
  })

  ### compareCohortCharacterizationAnalysisNameFilter -----
  shiny::observe({
    characterizationAnalysisOptionsUniverse <- NULL
    charcterizationAnalysisOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      characterizationAnalysisOptionsUniverse <- analysisNameOptions
      charcterizationAnalysisOptionsSelected <-
        temporalAnalysisRef %>%
          dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
          dplyr::pull(.data$analysisName) %>%
          unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "compareCohortCharacterizationAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = characterizationAnalysisOptionsUniverse,
      selected = charcterizationAnalysisOptionsSelected
    )
  })

  ## ReactiveVal: compareCohortcharacterizationDomainIdFilter ----
  compareCohortcharacterizationDomainIdFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$compareCohortcharacterizationDomainIdFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$compareCohortcharacterizationDomainIdFilter_open) ||
      !is.null(input$tabs)) {
      compareCohortcharacterizationDomainIdFilter(input$compareCohortcharacterizationDomainIdFilter)
    }
  })
  ### compareCohortcharacterizationDomainIdFilter -----
  shiny::observe({
    characterizationDomainOptionsUniverse <- NULL
    charcterizationDomainOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      characterizationDomainOptionsUniverse <- domainIdOptions
      charcterizationDomainOptionsSelected <-
        temporalAnalysisRef %>%
          dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
          dplyr::pull(.data$domainId) %>%
          unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "compareCohortcharacterizationDomainIdFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = characterizationDomainOptionsUniverse,
      selected = charcterizationDomainOptionsSelected
    )
  })

  ## compareCohortCharacterizationDataFiltered ------------
  compareCohortCharacterizationDataFiltered <- shiny::reactive({
    if (!input$tabs %in% c("compareCohortCharacterization")) {
      return(NULL)
    }
    validate(need(length(input$database) == 1, "One data source must be selected"))
    validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
    validate(need(
      length(comparatorCohortId()) == 1,
      "One comparator cohort must be selected"
    ))
    validate(
      need(
        targetCohortId() != comparatorCohortId(),
        "Target and comparator cohorts cannot be the same"
      )
    )
    data <- characterizationOutputForCompareCharacterizationMenu()
    if (!hasData(data)) {
      return(NULL)
    }

    data <- data$covariateValue
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
      dplyr::filter(.data$timeId %in% c(characterizationTimeIdChoices$timeId %>% unique(), NA)) %>%
      dplyr::filter(.data$cohortId %in% c(targetCohortId(), comparatorCohortId())) %>%
      dplyr::filter(.data$databaseId %in% c(input$database))

    if (input$charCompareType == "Raw") {
      if (input$compareCharacterizationProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y")
      } else if (input$compareCharacterizationProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "N")
      }
    }

    if (input$compareCharacterizationProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "Y")
    } else if (input$compareCharacterizationProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "N")
    }

    data <- data %>%
      dplyr::filter(.data$analysisName %in% compareCohortCharacterizationAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% compareCohortcharacterizationDomainIdFilter())

    if (hasData(selectedConceptSets())) {
      if (hasData(getResolvedAndMappedConceptIdsForFilters())) {
        data <- data %>%
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      }
    }
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })

  ## compareCohortCharacterizationBalanceData ----------------------------------------
  compareCohortCharacterizationBalanceData <- shiny::reactive({
    if (!input$tabs %in% c("compareCohortCharacterization")) {
      return(NULL)
    }
    data <- compareCohortCharacterizationDataFiltered()
    if (!hasData(data)) {
      return(NULL)
    }

    covs1 <- data %>%
      dplyr::filter(.data$cohortId %in% c(targetCohortId()))
    if (!hasData(covs1)) {
      return(NULL)
    }
    covs2 <- data %>%
      dplyr::filter(.data$cohortId %in% c(comparatorCohortId()))
    if (!hasData(covs2)) {
      return(NULL)
    }

    balance <- compareCohortCharacteristics(covs1, covs2)
    return(balance)
  })

  ## compareCohortCharacterizationPrettyTable ----------------------------------------
  compareCohortCharacterizationPrettyTable <- shiny::reactive(x = {
    if (!input$charCompareType == "Pretty table") {
      return(NULL)
    }
    data <- compareCohortCharacterizationBalanceData()
    if (!hasData(data)) {
      return(NULL)
    }

    showDataAsPercent <- TRUE

    if (showDataAsPercent) {
      data1 <- data %>%
        dplyr::rename(
          "cohortId" = .data$cohortId1,
          "mean" = .data$mean1,
          "sumValue" = .data$sumValue1
        ) %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$mean
        ) %>%
        dplyr::rename(sumValue = .data$mean)

      data2 <- data %>%
        dplyr::rename(
          "cohortId" = .data$cohortId2,
          "mean" = .data$mean2,
          "sumValue" = .data$sumValue2
        ) %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$mean
        ) %>%
        dplyr::rename(sumValue = .data$mean)
    } else {
      data1 <- data %>%
        dplyr::rename(
          "cohortId" = .data$cohortId1,
          "mean" = .data$mean1,
          "sumValue" = .data$sumValue1
        ) %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$sumValue
        )
      data2 <- data %>%
        dplyr::rename(
          "cohortId" = .data$cohortId2,
          "mean" = .data$mean2,
          "sumValue" = .data$sumValue2
        ) %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$sumValue
        )
    }

    data1 <-
      prepareTable1(
        covariates = data1,
        prettyTable1Specifications = prettyTable1Specifications,
        cohort = cohort
      )

    data2 <-
      prepareTable1(
        covariates = data2,
        prettyTable1Specifications = prettyTable1Specifications,
        cohort = cohort
      )

    data <- data1 %>%
      dplyr::full_join(data2,
                       by = c(
                         "characteristic",
                         "sequence",
                         "databaseId"
                       )
      ) %>%
      dplyr::arrange(.data$databaseId, .data$sequence) %>%
      dplyr::select(-.data$databaseId)

    if (!hasData(data)) {
      return(NULL)
    }
    keyColumns <- c("characteristic")
    dataColumns <- intersect(
      x = colnames(data),
      y = cohort$shortName
    )

    table <- getDisplayTableSimple(
      data = data,
      keyColumns = keyColumns,
      dataColumns = dataColumns,
      showDataAsPercent = showDataAsPercent
    )
    return(table)
  })

  ## compareCohortCharacterizationRawTable ----------------------------------------
  compareCohortCharacterizationRawTable <- shiny::reactive(x = {
    if (!input$charCompareType == "Raw table") {
      return(NULL)
    }
    data <- compareCohortCharacterizationBalanceData()
    if (!hasData(data)) {
      return(NULL)
    }

    distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
    sortedTemporalChoices <- data %>%
      dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
      dplyr::distinct(.data$temporalChoices) %>%
      dplyr::pull(.data$temporalChoices)

    data <- data %>%
      dplyr::arrange(factor(.data$temporalChoices, levels = sortedTemporalChoices))
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = "Post processing: Rendering table",
      value = 0
    )
    data <- data %>%
      dplyr::rename(
        "target" = mean1,
        "sdT" = sd1,
        "comparator" = mean2,
        "sdC" = sd2,
        "StdDiff" = absStdDiff
      )

    keyColumnFields <-
      c("covariateName", "analysisName", "conceptId")

    showDataAsPercent <- FALSE
    if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation") {
      dataColumnFields <-
        c(
          "target",
          "sdT",
          "comparator",
          "sdC",
          "StdDiff"
        )
    } else {
      dataColumnFields <- c("target", "comparator", "StdDiff")
      if (input$compareCharacterizationProportionOrContinuous == "Proportion") {
        showDataAsPercent <- TRUE
      }
    }
    countLocation <- 1

    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(
        data = data,
        string = dataColumnFields
      )

    getDisplayTableGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = NULL,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showDataAsPercent = showDataAsPercent,
      excludedColumnFromPercentage = "StdDiff",
      sort = TRUE,
      isTemporal = TRUE,
      pageSize = 100
    )
  })

  ## output: compareCohortCharacterizationTable ----------------------------------------
  output$compareCohortCharacterizationTable <- reactable::renderReactable(expr = {
    if (input$charCompareType == "Pretty table") {
      data <- compareCohortCharacterizationPrettyTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    } else if (input$charCompareType == "Raw table") {
      data <- compareCohortCharacterizationRawTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    }
  })

  ## output: compareCohortCharacterizationBalancePlot ----------------------------------------
  output$compareCohortCharacterizationBalancePlot <-
    ggiraph::renderggiraph(expr = {
      if (!input$charCompareType == "Plot") {
        return(NULL)
      }
      data <- compareCohortCharacterizationBalanceData()
      validate(need(
        hasData(data),
        "No data available for selected combination."
      ))

      distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
      data <- data %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
        dplyr::mutate(temporalChoices = factor(.data$temporalChoices, levels = unique(.data$temporalChoices)))

      plot <-
        plotTemporalCompareStandardizedDifference(
          balance = data,
          shortNameRef = cohort,
          xLimitMin = 0,
          xLimitMax = 1,
          yLimitMin = 0,
          yLimitMax = 1
        )
      validate(need(
        !is.null(plot),
        "No plot available for selected combination."
      ))
      return(plot)
    })

  # Compare Temporal Characterization.-----------------------------------------
  ## ReactiveVal: temporalCompareAnalysisNameFilter ----
  temporalCompareAnalysisNameFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$temporalCompareAnalysisNameFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$temporalCompareAnalysisNameFilter_open) ||
      !is.null(input$tabs)) {
      temporalCompareAnalysisNameFilter(input$temporalCompareAnalysisNameFilter)
    }
  })

  ### temporalCompareAnalysisNameFilter ----
  shiny::observe({
    temporalCharacterizationAnalysisOptionsUniverse <- NULL
    temporalCharcterizationAnalysisOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      temporalCharacterizationAnalysisOptionsUniverse <-
        analysisNameOptions
      temporalCharcterizationAnalysisOptionsSelected <-
        temporalAnalysisRef %>%
          dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
          dplyr::pull(.data$analysisName) %>%
          unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCompareAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = temporalCharacterizationAnalysisOptionsUniverse,
      selected = temporalCharcterizationAnalysisOptionsSelected
    )
  })

  ## ReactiveVal: temporalCompareDomainNameFilter ----
  temporalCompareDomainNameFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(
      input$temporalCompareDomainNameFilter_open,
      input$tabs
    )
  }, handlerExpr = {
    if (isFALSE(input$temporalCompareDomainNameFilter_open) ||
      !is.null(input$tabs)) {
      temporalCompareDomainNameFilter(input$temporalCompareDomainNameFilter)
    }
  })
  ### temporalCompareDomainNameFilter ----
  shiny::observe({
    temporalCharacterizationDomainOptionsUniverse <- NULL
    temporalCharcterizationDomainOptionsSelected <- NULL

    if (hasData(temporalAnalysisRef)) {
      temporalCharacterizationDomainOptionsUniverse <-
        domainIdOptions
      temporalCharcterizationDomainOptionsSelected <-
        temporalAnalysisRef %>%
          dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
          dplyr::pull(.data$domainId) %>%
          unique()
    }

    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCompareDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = temporalCharacterizationDomainOptionsUniverse,
      selected = temporalCharcterizationDomainOptionsSelected
    )
  })


  ## compareTemporalCharacterizationDataFiltered ------------
  compareTemporalCharacterizationDataFiltered <- shiny::reactive({
    if (!input$tabs %in% c("compareTemporalCharacterization")) {
      return(NULL)
    }
    validate(need(length(input$database) == 1, "One data source must be selected"))
    validate(need(
      length(targetCohortId()) == 1,
      "One target cohort must be selected"
    ))
    validate(need(
      length(comparatorCohortId()) == 1,
      "One comparator cohort must be selected"
    ))
    validate(
      need(
        targetCohortId() != comparatorCohortId(),
        "Target and comparator cohorts cannot be the same"
      )
    )
    if (!hasData(selectedTemporalTimeIds())) {
      return(NULL)
    }
    data <-
      characterizationOutputForCompareTemporalCharacterizationMenu()
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data$covariateValue
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
      dplyr::filter(.data$timeId %in% selectedTemporalTimeIds()) %>%
      dplyr::filter(.data$cohortId %in% c(targetCohortId(), comparatorCohortId())) %>%
      dplyr::filter(.data$databaseId %in% c(input$database))

    if (input$temporalCompareCharacterizationProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "Y")
    } else if (input$temporalCompareCharacterizationProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == "N")
    }

    data <- data %>%
      dplyr::filter(.data$analysisName %in% temporalCompareAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% temporalCompareDomainNameFilter())

    if (hasData(selectedConceptSets())) {
      if (hasData(getResolvedAndMappedConceptIdsForFilters())) {
        data <- data %>%
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      }
    }
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })


  ## compareCohortTemporalCharacterizationBalanceData ---------------------------
  compareCohortTemporalCharacterizationBalanceData <-
    shiny::reactive({
      if (!input$tabs %in% c("compareTemporalCharacterization")) {
        return(NULL)
      }
      data <- compareTemporalCharacterizationDataFiltered()
      if (!hasData(data)) {
        return(NULL)
      }
      covs1 <- data %>%
        dplyr::filter(.data$cohortId == targetCohortId())
      if (!hasData(covs1)) {
        return(NULL)
      }
      covs2 <- data %>%
        dplyr::filter(.data$cohortId %in% c(comparatorCohortId()))
      if (!hasData(covs2)) {
        return(NULL)
      }
      balance <-
        compareCohortCharacteristics(covs1, covs2)
      if (!hasData(balance)) {
        return(NULL)
      }
      return(balance)
    })


  ## compareCohortTemporalCharacterizationTable ---------------------------
  compareCohortTemporalCharacterizationTable <-
    shiny::reactive({
      if (!input$temporalCharacterizationType == "Raw table") {
        return(NULL)
      }
      data <- compareCohortTemporalCharacterizationBalanceData()
      validate(need(
        hasData(data),
        "No data available for selected combination."
      ))

      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Post processing: Rendering table",
        value = 0
      )

      distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
      sortedTemporalChoices <- data %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
        dplyr::distinct(.data$temporalChoices) %>%
        dplyr::pull(.data$temporalChoices)

      data <- data %>%
        dplyr::rename(
          "target" = mean1,
          "sDTarget" = sd1,
          "comparator" = mean2,
          "sDComparator" = sd2,
          "stdDiff" = stdDiff
        ) %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = sortedTemporalChoices))

      showDataAsPercent <- FALSE
      keyColumnFields <-
        c("covariateId", "covariateName", "analysisName")
      if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation") {
        dataColumnFields <-
          c(
            "target",
            "sDTarget",
            "comparator",
            "sDComparator",
            "stdDiff"
          )
      } else {
        dataColumnFields <- c("target", "comparator", "stdDiff")

        if (input$temporalCompareCharacterizationProportionOrContinuous == "Proportion") {
          showDataAsPercent <- TRUE
        }
      }

      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = data,
          string = dataColumnFields
        )

      table <- getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = NULL,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = showDataAsPercent,
        excludedColumnFromPercentage = "stdDiff",
        sort = TRUE,
        isTemporal = TRUE,
        pageSize = 100
      )
      return(table)
    })

  ## Output: temporalCharacterizationCompareTable ---------------------------
  output$temporalCharacterizationCompareTable <-
    reactable::renderReactable(expr = {
      data <- compareCohortTemporalCharacterizationTable()
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })

  ## Output: temporalCharacterizationComparePlot ---------------------------
  output$temporalCharacterizationComparePlot <- ggiraph::renderggiraph(expr = {
    if (!input$temporalCharacterizationType == "Plot") {
      return(NULL)
    }
    data <- compareCohortTemporalCharacterizationBalanceData()
    validate(need(
      hasData(data),
      "No data available for selected combination."
    ))
    validate(need(
      (nrow(data) - nrow(data[data$mean1 < 0.001,])) > 5 &&
        (nrow(data) - nrow(data[data$mean2 < 0.001,])) > 5,
      paste0("Values of the means are too low.")
    ))

    distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
    data <- data %>%
      dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
      dplyr::mutate(temporalChoices = factor(.data$temporalChoices, levels = unique(.data$temporalChoices)))
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = "Plotting ",
      value = 0
    )
    plot <-
      plotTemporalCompareStandardizedDifference(
        balance = data,
        shortNameRef = cohort,
        xLimitMin = 0,
        xLimitMax = 1,
        yLimitMin = 0,
        yLimitMax = 1
      )
    return(plot)
  })

  # Login User ---------------------------------------------
  activeLoggedInUser <- reactiveVal(NULL)

  if (enableAnnotation &&
    exists("userCredentials") &&
    nrow(userCredentials) > 0) {
    shiny::observeEvent(
      eventExpr = input$annotationUserPopUp,
      handlerExpr = {
        shiny::showModal(
          shiny::modalDialog(
            title = "Annotate",
            easyClose = TRUE,
            size = "s",
            footer = tagList(
              shiny::actionButton(inputId = "login", label = "Login"),
              shiny::modalButton("Cancel")
            ),
            tags$div(
              shiny::textInput(
                inputId = "userName",
                label = "User Name",
                width = NULL,
                value = if (enableAuthorization) {
                  ""
                } else {
                  "annonymous"
                }
              ),
              if (enableAuthorization) {
                shiny::passwordInput(
                  inputId = "password",
                  label = "Local Password",
                  width = NULL
                )
              },
            )
          )
        )
      }
    )


    shiny::observeEvent(
      eventExpr = input$login,
      handlerExpr = {
        tryCatch(
          expr = {
            if (enableAuthorization == TRUE) {
              if (input$userName == "" || input$password == "") {
                activeLoggedInUser(NULL)
                shiny::showModal(
                  shiny::modalDialog(
                    title = "Error",
                    easyClose = TRUE,
                    size = "s",
                    fade = TRUE,
                    "Please enter both the fields"
                  )
                )
              }
              userCredentialsFiltered <- userCredentials %>%
                dplyr::filter(.data$userId == input$userName)
              if (nrow(userCredentialsFiltered) > 0) {
                passwordHash <-
                  digest::digest(input$password, algo = "sha512")
                if (passwordHash %in% userCredentialsFiltered$hashCode) {
                  activeLoggedInUser(input$userName)
                  shiny::removeModal()
                } else {
                  activeLoggedInUser(NULL)
                  shiny::showModal(
                    shiny::modalDialog(
                      title = "Error",
                      easyClose = TRUE,
                      size = "s",
                      fade = TRUE,
                      "Invalid User"
                    )
                  )
                }
              } else {
                activeLoggedInUser(NULL)
                shiny::showModal(
                  shiny::modalDialog(
                    title = "Error",
                    easyClose = TRUE,
                    size = "s",
                    fade = TRUE,
                    "Invalid User"
                  )
                )
              }
            } else {
              if (input$userName == "") {
                activeLoggedInUser(NULL)
                shiny::showModal(
                  shiny::modalDialog(
                    title = "Error",
                    easyClose = TRUE,
                    size = "s",
                    fade = TRUE,
                    "Please enter the user name."
                  )
                )
              } else {
                activeLoggedInUser(input$userName)
                shiny::removeModal()
              }
            }
          },
          error = function() {
            activeLoggedInUser(NULL)
          }
        )
      }
    )

    output$userNameLabel <- shiny::renderText({
      return(ifelse(
        is.null(activeLoggedInUser()),
        "",
        paste(
          as.character(icon("user-circle")),
          stringr::str_to_title(activeLoggedInUser())
        )
      ))
    })

    # Annotation Section ------------------------------------
    ## Annotation enabled ------
    output$postAnnotationEnabled <- shiny::reactive({
      return(!is.null(activeLoggedInUser()) & enableAnnotation)
    })
    shiny::outputOptions(
      x = output,
      name = "postAnnotationEnabled",
      suspendWhenHidden = FALSE
    )

    ## Retrieve Annotation ----------------
    reloadAnnotationSection <- reactiveVal(0)
    getAnnotationReactive <- shiny::reactive({
      reloadAnnotationSection()
      if (input$tabs == "cohortCounts" |
        input$tabs == "cohortOverlap" |
        input$tabs == "incidenceRate" |
        input$tabs == "timeDistribution") {
        selectedCohortIds <- cohort %>%
          dplyr::filter(.data$compoundName %in% c(input$cohorts)) %>% # many cohorts selected
          dplyr::pull(.data$cohortId)
      } else {
        selectedCohortIds <- cohort %>%
          dplyr::filter(.data$compoundName %in% c(input$targetCohort)) %>% # one cohort selected
          dplyr::pull(.data$cohortId)
      }
      results <- getAnnotationResult(
        dataSource = dataSource,
        diagnosticsId = input$tabs,
        cohortIds = selectedCohortIds,
        databaseIds = selectedDatabaseIds()
      )

      if (nrow(results$annotation) == 0) {
        return(NULL)
      }
      return(results)
    })


    ## renderedAnnotation ----
    renderedAnnotation <- shiny::reactiveVal()
    shiny::observeEvent(eventExpr = input$tabs, {
      if (!is.null(input$tabs)) {
        renderedAnnotation(callModule(
          markdownInput::moduleMarkdownInput,
          paste0("annotation", input$tabs)
        ))

        output[[paste0("output", input$tabs)]] <-
          reactable::renderReactable({
            results <- getAnnotationReactive()

            if (is.null(results)) {
              return(NULL)
            }
            data <- results$annotation
            for (i in 1:nrow(data)) {
              data[i,]$annotation <-
                markdown::renderMarkdown(text = data[i,]$annotation)
            }
            data <- data %>%
              dplyr::mutate(
                Annotation = paste0(
                  "<b>",
                  .data$createdBy,
                  "@",
                  getTimeFromInteger(.data$createdOn),
                  ":</b>",
                  .data$annotation
                )
              ) %>%
              dplyr::select(.data$annotationId, .data$Annotation)

            reactable::reactable(
              data,
              columns = list(
                annotationId = reactable::colDef(show = FALSE),
                Annotation = reactable::colDef(html = TRUE)
              ),
              details = function(index) {
                subTable <- results$annotationLink %>%
                  dplyr::filter(.data$annotationId == data[index,]$annotationId) %>%
                  dplyr::inner_join(cohort %>%
                                      dplyr::select(
                                        .data$cohortId,
                                        .data$cohortName
                                      ),
                                    by = "cohortId"
                  )
                distinctCohortName <- subTable %>%
                  dplyr::distinct(.data$cohortName)
                distinctDatabaseId <- subTable %>%
                  dplyr::distinct(.data$databaseId)

                htmltools::div(
                  style = "margin:0;padding:0;padding-left:50px;",
                  tags$p(
                    style = "margin:0;padding:0;",
                    "Related Cohorts: ",
                    tags$p(
                      style = "padding-left:30px;",
                      tags$pre(
                        paste(distinctCohortName$cohortName, collapse = "\n")
                      )
                    )
                  ),
                  tags$br(),
                  tags$p(
                    "Related Databses: ",
                    tags$p(
                      style = "padding-left:30px;",
                      tags$pre(
                        paste(distinctDatabaseId$databaseId, collapse = "\n")
                      )
                    )
                  )
                )
              }
            )
          })
      }
    })


    ## Post Annotation ----------------
    getParametersToPostAnnotation <- shiny::reactive({
      tempList <- list()
      tempList$diagnosticsId <- input$tabs

      # Annotation - cohort Ids
      if (!is.null(input[[paste0("cohort", input$tabs)]])) {
        selectedCohortIds <-
          cohort %>%
            dplyr::filter(.data$compoundName %in% input[[paste0("cohort", input$tabs)]]) %>%
            dplyr::pull(.data$cohortId)
        # cohortsConceptInDataSource should be the same as in menu cohort
      } else {
        selectedCohortIds <- input$targetCohort
      }
      tempList$cohortIds <- selectedCohortIds

      # Annotation - database Ids
      if (!is.null(input[[paste0("database", input$tabs)]])) {
        selectedDatabaseIds <- input[[paste0("database", input$tabs)]]
      } else {
        selectedDatabaseIds <- selectedDatabaseIds()
      }
      tempList$databaseIds <- selectedDatabaseIds
      return(tempList)
    })

    postAnnotationTabList <- reactiveVal(c())
    observeEvent(eventExpr = input[[paste0("postAnnotation", input$tabs)]], {
      if (!paste(toString(input[[paste0("postAnnotation", input$tabs)]]), input$tabs) %in% postAnnotationTabList()) {
        postAnnotationTabList(c(
          postAnnotationTabList(),
          paste(toString(input[[paste0("postAnnotation", input$tabs)]]), input$tabs)
        ))
      }
    })

    observeEvent(
      eventExpr = postAnnotationTabList(),
      handlerExpr = {
        parametersToPostAnnotation <- getParametersToPostAnnotation()
        annotation <-
          renderedAnnotation()() # ()() - This is to retrieve a function inside reactive
        if (!is.null(activeLoggedInUser())) {
          createdBy <- activeLoggedInUser()
        } else {
          createdBy <- "Unknown"
        }

        result <- postAnnotationResult(
          dataSource = dataSource,
          resultsDatabaseSchema = resultsDatabaseSchema,
          diagnosticsId = parametersToPostAnnotation$diagnosticsId,
          cohortIds = parametersToPostAnnotation$cohortIds,
          databaseIds = parametersToPostAnnotation$databaseIds,
          annotation = annotation,
          createdBy = createdBy,
          createdOn = getTimeAsInteger()
        )

        if (result) {
          # trigger reload
          reloadAnnotationSection(reloadAnnotationSection() + 1)
        }
      }
    )
  }
  # Infoboxes -------------------
  showInfoBox <- function(title, htmlFileName) {
    shiny::showModal(shiny::modalDialog(
      title = title,
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(readChar(
        htmlFileName, file.info(htmlFileName)$size
      ))
    ))
  }

  shiny::observeEvent(input$cohortCountsInfo, {
    showInfoBox("Cohort Counts", "html/cohortCounts.html")
  })

  shiny::observeEvent(input$incidenceRateInfo, {
    showInfoBox("Incidence Rate", "html/incidenceRate.html")
  })

  shiny::observeEvent(input$timeDistributionInfo, {
    showInfoBox("Time Distributions", "html/timeDistribution.html")
  })

  shiny::observeEvent(input$conceptsInDataSourceInfo, {
    showInfoBox(
      "Concepts in data source",
      "html/conceptsInDataSource.html"
    )
  })

  shiny::observeEvent(input$orphanConceptsInfo, {
    showInfoBox("Orphan (Source) Concepts", "html/orphanConcepts.html")
  })

  shiny::observeEvent(input$conceptSetDiagnosticsInfo, {
    showInfoBox(
      "Concept Set Diagnostics",
      "html/conceptSetDiagnostics.html"
    )
  })

  shiny::observeEvent(input$inclusionRuleStatsInfo, {
    showInfoBox(
      "Inclusion Rule Statistics",
      "html/inclusionRuleStats.html"
    )
  })

  shiny::observeEvent(input$indexEventBreakdownInfo, {
    showInfoBox("Index Event Breakdown", "html/indexEventBreakdown.html")
  })

  shiny::observeEvent(input$visitContextInfo, {
    showInfoBox("Visit Context", "html/visitContext.html")
  })

  shiny::observeEvent(input$cohortCharacterizationInfo, {
    showInfoBox(
      "Cohort Characterization",
      "html/cohortCharacterization.html"
    )
  })

  shiny::observeEvent(input$temporalCharacterizationInfo, {
    showInfoBox(
      "Temporal Characterization",
      "html/temporalCharacterization.html"
    )
  })

  shiny::observeEvent(input$cohortOverlapInfo, {
    showInfoBox("Cohort Overlap", "html/cohortOverlap.html")
  })

  shiny::observeEvent(input$compareCohortCharacterizationInfo, {
    showInfoBox(
      "Compare Cohort Characteristics",
      "html/compareCohortCharacterization.html"
    )
  })

  # Cohort labels ---------------------------------------
  targetCohortCount <- shiny::reactive({
    targetCohortWithCount <-
      getResultsCohortCounts(
        dataSource = dataSource,
        cohortIds = targetCohortId(),
        databaseIds = selectedDatabaseIds()
      ) %>%
        dplyr::left_join(y = cohort, by = "cohortId") %>%
        dplyr::arrange(.data$cohortName)
    return(targetCohortWithCount)
  })

  targetCohortCountHtml <- shiny::reactive({
    targetCohortCount <- targetCohortCount()
    return(htmltools::withTags(div(
      h5(
        "Target: ",
        targetCohortCount$cohortName,
        " ( n = ",
        scales::comma(x = targetCohortCount$cohortSubjects),
        " )"
      )
    )))
  })

  selectedCohorts <- shiny::reactive({
    cohorts <- cohortSubset() %>%
      dplyr::filter(.data$cohortId %in% cohortIds()) %>%
      dplyr::arrange(.data$cohortId) %>%
      dplyr::select(.data$compoundName)
    return(apply(cohorts, 1, function(x) {
      tags$tr(lapply(x, tags$td))
    }))
  })

  selectedCohort <- shiny::reactive({
    return(input$targetCohort)
  })

  selectedComparatorCohort <- shiny::reactive({
    return(input$comparatorCohort)
  })


  output$cohortCountsSelectedCohorts <-
    shiny::renderUI({
      selectedCohorts()
    })
  output$inclusionRuleStatSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$orphanConceptsSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$indexEventBreakdownSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$characterizationSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$inclusionRuleStatSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })

  output$incidenceRateSelectedCohorts <-
    shiny::renderUI({
      selectedCohorts()
    })

  output$temporalCharacterizationSelectedCohort <-
    shiny::renderUI({
      return(selectedCohort())
    })

  output$temporalCharacterizationSelectedDatabase <-
    shiny::renderUI({
      return(selectedDatabaseIds())
    })

  output$cohortCharCompareSelectedCohort <- shiny::renderUI({
    htmltools::withTags(table(
      tr(td(
        selectedCohort()
      )),
      tr(td(
        selectedComparatorCohort()
      ))
    ))
  })

  output$cohortCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(selectedDatabaseIds())
    })

  output$temporalCharCompareSelectedCohort <-
    shiny::renderUI({
      htmltools::withTags(table(
        tr(td(
          selectedCohort()
        )),
        tr(td(
          selectedComparatorCohort()
        ))
      ))
    })

  output$temporalCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(selectedDatabaseIds())
    })


  cohortOverlapModule(id = "cohortOverlap",
                      dataSource = dataSource,
                      selectedCohorts = selectedCohorts,
                      selectedDatabaseIds = selectedDatabaseIds,
                      targetCohortId = targetCohortId,
                      cohortIds = cohortIds,
                      cohortTable = cohort)

  visitContextModule(id = "visitContext",
                     dataSource = dataSource,
                     selectedCohort = selectedCohort,
                     selectedDatabaseIds = selectedDatabaseIds,
                     targetCohortId = targetCohortId,
                     cohortTable = cohort,
                     databaseTable = database)

  conceptsInDataSourceModule(id = "conceptsInDataSource",
                             dataSource = dataSource,
                             selectedCohort = selectedCohort,
                             selectedDatabaseIds = selectedDatabaseIds,
                             targetCohortId = targetCohortId,
                             selectedConceptSets = selectedConceptSets,
                             getResolvedAndMappedConceptIdsForFilters = getResolvedAndMappedConceptIdsForFilters,
                             cohortTable = cohort,
                             databaseTable = database)

  incidenceRatesModule(id = "incidenceRates",
                       dataSource = dataSource,
                       selectedCohorts = selectedCohorts,
                       cohortIds = cohortIds,
                       selectedDatabaseIds = selectedDatabaseIds,
                       cohortTable = cohort)

  timeDistributionsModule(id = "timeDistributions",
                          dataSource = dataSource,
                          selectedCohorts = selectedCohorts,
                          cohortIds = cohortIds,
                          selectedDatabaseIds = selectedDatabaseIds,
                          cohortTable = cohort)

  databaseInformationModule(id = "databaseInformation",
                            dataSource = dataSource,
                            selectedDatabaseIds = selectedDatabaseIds,
                            databaseTable = database)
})
