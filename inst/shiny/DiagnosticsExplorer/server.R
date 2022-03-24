shiny::shinyServer(function(input, output, session) {
  cohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$cohort])
  })
  
  cohortIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$cohorts_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$cohorts_open) || !is.null(input$tabs)) {
      selectedCohortIds <-
        cohort$cohortId[cohort$compoundName  %in% input$cohorts]
      cohortIds(selectedCohortIds)
    }
  })
  
  comparatorCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$comparatorCohort])
  })
  
  # conceptSetIds ---------------------------------------------------------
  conceptSetIds <- shiny::reactive(x = {
    conceptSetsFiltered <- conceptSets %>% 
      dplyr::filter(.data$conceptSetName %in% input$conceptSetsSelected) %>% 
      dplyr::filter(.data$cohortId %in% cohortId()) %>% 
      dplyr::select(.data$conceptSetId) %>% 
      dplyr::pull() %>% 
      unique()
    return(conceptSetsFiltered)
  })
  
  timeIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$timeIdChoices_open,
         input$tabs)
  }, handlerExpr = {
    if (exists('temporalCovariateChoices') &&
        (isFALSE(input$timeIdChoices_open) ||
         !is.null(input$tabs))) {
      selectedTimeIds <- temporalCovariateChoices %>%
        dplyr::filter(choices %in% input$timeIdChoices) %>%
        dplyr::pull(timeId)
      timeIds(selectedTimeIds)
    }
  })
  
  databaseIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$databases_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$databases_open) || !is.null(input$tabs)) {
      selectedDatabaseIds <- input$databases
      databaseIds(selectedDatabaseIds)
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
      inputId = "cohort",
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
    if (input$tabs == 'cohortCounts' |
        input$tabs == 'cohortOverlap' |
        input$tabs == 'incidenceRate' |
        input$tabs == 'timeDistribution') {
      subset <- input$cohorts
    } else {
      subset <- input$cohort
    }
    
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = paste0("cohort", input$tabs),
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
      choices = databaseIds(),
      selected = databaseIds()
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
    data <-  cohortSubset() %>%
      dplyr::select(cohort = .data$shortName, .data$cohortId, .data$cohortName)
    return(data)
  })
  
  # Cohort Definition ---------------------------------------------------------
  output$cohortDefinitionTable <-
    reactable::renderReactable(expr = {
      data <- cohortDefinitionTableData()  %>%
        dplyr::mutate(cohortId = as.character(.data$cohortId))
      
      validate(need(hasData(data), "There is no data for this cohort."))
      keyColumns <- c("cohort", "cohortId", "cohortName")
      dataColumns <- c()
      
      displayTable <- getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        selection = "single"
      )
      return(displayTable)
    })
  
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
  
  output$cohortDefinitionRowIsSelected <- reactive({
    return(!is.null(selectedCohortDefinitionRow()))
  })
  
  outputOptions(output,
                "cohortDefinitionRowIsSelected",
                suspendWhenHidden = FALSE)
  
  ## cohortDetailsText ---------------------------------------------------------
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
  
  ## cohortCountsTableInCohortDefinition ---------------------------------------------------------
  output$cohortCountsTableInCohortDefinition <-
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
        dplyr::select(.data$databaseId,
                      .data$cohortSubjects,
                      .data$cohortEntries) %>% 
        dplyr::rename("persons" = .data$cohortSubjects,
                      "events" = .data$cohortEntries)
      
      validate(need(hasData(data), "There is no data for this cohort."))
      
      keyColumns <- c("databaseId")
      dataColumns <- c("persons", "events")
      
      displayTable <- getDisplayTableSimple(data = data,
                                            keyColumns = keyColumns,
                                            dataColumns = dataColumns)
      return(displayTable)
    })
  
  ## cohortDefinitionCirceRDetails ---------------------------------------------------------
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
        embedText = paste0("Generated for cohort id:",
                           data$cohortId[1], " on ", Sys.time()),
        includeConceptSets = TRUE
      )
    return(details)
  })
  
  output$cohortDefinitionText <- shiny::renderUI(expr = {
    cohortDefinitionCirceRDetails()$cohortHtmlExpression %>%
      shiny::HTML()
  })
  ## cohortDefinitionJson ---------------------------------------------------------
  output$cohortDefinitionJson <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  ## cohortDefinitionSql ---------------------------------------------------------
  output$cohortDefinitionSql <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$sql
    }
  })
  
  ## cohortDefinitionConceptSetExpression ---------------------------------------------------------
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
      all(!is.null(data),
          nrow(data) > 0),
      "There is no data for this cohort."
    ))
    
    keyColumns <- c("id","name")
    dataColumns <- c()
    getDisplayTableSimple(
      data = data,
      keyColumns = keyColumns,
      dataColumns = dataColumns,
      selection = "single"
    )
  })
  
  ### cohortDefinitionConceptSetExpressionSelected ---------------------------------------------------------
  cohortDefinitionConceptSetExpressionSelected <- shiny::reactive(x = {
    idx <- reactable::getReactableState("conceptsetExpressionsInCohort", "selected")
    if (length(idx) == 0 || is.null(idx)) {
      return(NULL)
    }
    if (hasData(cohortDefinitionConceptSetExpression()$conceptSetExpression)) {
      data <-
        cohortDefinitionConceptSetExpression()$conceptSetExpression[idx, ]
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
  
  shiny::outputOptions(x = output,
                       name = "cohortDefinitionConceptSetExpressionRowIsSelected",
                       suspendWhenHidden = FALSE)
  
  output$isDataSourceEnvironment <- shiny::reactive(x = {
    return(is(dataSource, "environment"))
  })
  shiny::outputOptions(x = output,
                       name = "isDataSourceEnvironment",
                       suspendWhenHidden = FALSE)
  
  ### cohortDefinitionConceptSetDetails ---------------------------------------------------------
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
        all(!is.null(data),
            nrow(data) > 0),
        "There is no data for this cohort."
      ))
      if (is.null(cohortDefinitionConceptSetDetails())) {
        return(NULL)
      }
      
      data <- data %>% 
        dplyr::rename(exclude = .data$isExcluded,
                      descendants = .data$includeDescendants,
                      mapped = .data$includeMapped,
                      invalid = .data$invalidReason)
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
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
      getDisplayTableSimple(data = data,
                            keyColumns = keyColumns,
                            dataColumns = dataColumns)
      
    })
  
  getDatabaseIdInCohortConceptSet <- shiny::reactive({
    return(database$databaseId[database$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema])
  })
  
  ## Cohort Concept Set
  ### getSubjectAndRecordCountForCohortConceptSet ---------------------------------------------------------
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
  
  ### subjectCountInCohortConceptSet ---------------------------------------------------------
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
  
  ### recordCountInCohortConceptSet ---------------------------------------------------------
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
  
  ### getCohortDefinitionResolvedConceptsReactive ---------------------------------------------------------
  getCohortDefinitionResolvedConceptsReactive <-
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      }
      output <-
        resolvedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortId =  row$cohortId
        )
      if (!hasData(output)) {
        return(NULL)
      }
      conceptCount <- getCountForConceptIdInCohortReactive()
      output <- output %>% 
        dplyr::left_join(conceptCount,
                         by = c("databaseId", "conceptId"))
      return(output)
    })
  
  output$cohortDefinitionResolvedConceptsTable <-
    reactable::renderReactable(expr = {
      if (input$conceptSetsType != 'Resolved') {
        return(NULL)
      }
      
      databaseIdToFilter <- database %>%
        dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
        dplyr::pull(.data$databaseId)
      if (!hasData(databaseIdToFilter)) {
        return(NULL)
      }
      
      validate(need(
        length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
        "Please select concept set"
      ))
      
      data <- getCohortDefinitionResolvedConceptsReactive()
      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))
      data <- data %>%
        dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
        dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
        dplyr::rename("subjects" = .data$conceptSubjects,
                      "count" = .data$conceptCount)
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
      dataColumns <- c("subjects",
                       "count")
      displayTable <- getDisplayTableSimple(data = data,
                                            keyColumns = keyColumns,
                                            dataColumns = dataColumns)
      return(displayTable)
    })
  
  ### getCohortDefinitionMappedConceptsReactive ---------------------------------------------------------
  getCohortDefinitionMappedConceptsReactive <-
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row)) {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = "Getting concepts mapped to concept ids resolved by concept set expression (may take time)", value = 0)
      output <-
        mappedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortId =  row$cohortId
        )
      if (!hasData(output)) {
        return(NULL)
      }
      conceptCount <- getCountForConceptIdInCohortReactive()
      output <- output %>% 
        dplyr::left_join(conceptCount,
                         by = c("databaseId", "conceptId"))
      return(output)
    })
  
  output$cohortDefinitionMappedConceptsTable <-
    reactable::renderReactable(expr = {
      if (input$conceptSetsType != 'Mapped') {
        return(NULL)
      }
      
      databaseIdToFilter <- database %>%
        dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
        dplyr::pull(.data$databaseId)
      if (!hasData(databaseIdToFilter)) {
        return(NULL)
      }
      
      validate(need(
        length(cohortDefinitionConceptSetExpressionSelected()$id) > 0,
        "Please select concept set"
      ))
      
      data <- getCohortDefinitionMappedConceptsReactive()
      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))
      
      data <- data %>%
        dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSelected()$id) %>%
        dplyr::filter(.data$databaseId == databaseIdToFilter) %>%
        dplyr::rename("subjects" = .data$conceptSubjects,
                      "count" = .data$conceptCount)
      validate(need(
        hasData(data),
        paste0("No data for database id ", input$databaseOrVocabularySchema)
      ))
      
      keyColumns <- c(
        "resolvedConceptId",
        "conceptId",
        "conceptName",
        "domainId",
        "vocabularyId",
        "conceptClassId",
        "standardConcept",
        "conceptCode"
      )
      dataColumns <- c("subjects",
                       "count")
      
      getDisplayTableSimple(data = data,
                            keyColumns = keyColumns,
                            dataColumns = dataColumns)
      
    })
  
  ### getCohortDefinitionOrphanConceptsReactive ---------------------------------------------------------
  getCohortDefinitionOrphanConceptsReactive <- shiny::reactive(x = {
    validate(need(
      all(
        !is.null(getDatabaseIdInCohortConceptSet()),
        length(getDatabaseIdInCohortConceptSet()) > 0
      ),
      "Orphan codes are not available for reference vocabulary in this version."
    ))
    if (is.null(row) ||
        length(cohortDefinitionConceptSetExpressionSelected()$name) == 0) {
      return(NULL)
    }
    validate(need(
      length(input$databaseOrVocabularySchema) > 0,
      "No data sources chosen"
    ))
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    }
    output <- getOrphanConceptResult(
      dataSource = dataSource,
      databaseIds = database$databaseId,
      cohortId = row$cohortId
    )
    if (!hasData(output)) {
      return(NULL)
    }
    output <- output %>% 
      dplyr::anti_join(getCohortDefinitionResolvedConceptsReactive() %>% 
                         dplyr::select(.data$conceptId) %>% 
                         dplyr::distinct(),
                       by = "conceptId")
    if (!hasData(output)) {
      return(NULL)
    }
    output <- output %>% 
      dplyr::anti_join(getCohortDefinitionMappedConceptsReactive() %>% 
                         dplyr::select(.data$conceptId) %>% 
                         dplyr::distinct(),
                       by = "conceptId")
    if (!hasData(output)) {
      return(NULL)
    }
    output <- output %>%
      dplyr::rename("persons" = .data$conceptSubjects,
                    "records" = .data$conceptCount)
    return(output)
  })
  
  output$cohortDefinitionOrphanConceptTable <-
    reactable::renderReactable(expr = {
      if (input$conceptSetsType != 'Orphan concepts') {
        return(NULL)
      }
      databaseIdToFilter <- database %>%
        dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
        dplyr::pull(.data$databaseId)
      if (!hasData(databaseIdToFilter)) {
        return(NULL)
      }
      data <- getCohortDefinitionOrphanConceptsReactive()
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
        c("conceptId",
          "conceptName",
          "vocabularyId",
          "conceptCode",
          "standard")
      dataColumns <- c("subjects",
                       "count")
      
      displayTable <- getDisplayTableSimple(data = data,
                                            keyColumns = keyColumns,
                                            dataColumns = dataColumns)
      return(displayTable)
    })
  
  # 
  #   
  #   ### resolveMappedConceptSetFromVocabularyDatabaseSchemaReactive ---------------------------------------------------------
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
  
  ### getCountForConceptIdInCohortReactive ---------------------------------------------------------
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
  
  ## cohortConceptsetExpressionJson ---------------------------------------------------------
  output$cohortConceptsetExpressionJson <- shiny::renderText({
    if (is.null(cohortDefinitionConceptSetExpressionSelected())) {
      return(NULL)
    }
    json <- cohortDefinitionConceptSetExpressionSelected()$json
    return(json)
  })
  
  ## Other ---------------------------------------------------------
  ### getConceptSetIds ---------------------------------------------------------
  getConceptSetIds <- shiny::reactive(x = {
    return(conceptSets$conceptSetId[conceptSets$conceptSetName  %in% input$conceptSetsSelected])
  })
  
  ### getResolvedConceptsReactive ---------------------------------------------------------
  getResolvedConceptsReactive <-
    shiny::reactive(x = {
      output <-
        resolvedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortId =  cohortId()
        )
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })
  
  ### getMappedConceptsReactive ---------------------------------------------------------
  getMappedConceptsReactive <-
    shiny::reactive(x = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = "Getting concepts mapped to concept ids resolved by concept set expression (may take time)", value = 0)
      output <-
        mappedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortId =  cohortId()
        )
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })
  
  ### getResolvedAndMappedConceptIdsForFilters ---------------------------------------------------------
  getResolvedAndMappedConceptIdsForFilters <- shiny::reactive({
    validate(need(hasData(databaseIds()), "No data sources chosen"))
    validate(need(hasData(cohortId()), "No cohort chosen"))
    validate(need(hasData(conceptSetIds()), "No concept set id chosen"))
    
    resolved <- getResolvedConceptsReactive()
    mapped <- getMappedConceptsReactive()
    output <- c()
    if (hasData(resolved)) {
      resolved <- resolved %>%
        dplyr::filter(.data$databaseId %in% databaseIds()) %>%
        dplyr::filter(.data$cohortId %in% cohortId()) %>%
        dplyr::filter(.data$conceptSetId %in% conceptSetIds())
      output <- c(output, resolved$conceptId) %>% unique()
    }
    if (hasData(mapped)) {
      mapped <- mapped %>%
        dplyr::filter(.data$databaseId %in% databaseIds()) %>%
        dplyr::filter(.data$cohortId %in% cohortId()) %>%
        dplyr::filter(.data$conceptSetId %in% conceptSetIds())
      output <- c(output, mapped$conceptId) %>% unique()
    }
    if (hasData(output)) {
      return(output)
    } else {
      return(NULL)
    }
  })
  
  # Cohort Counts ---------------------------------------------------------------------------
  getCohortCountResultReactive <- shiny::reactive(x = {
    if (!input$tabs == "cohortCounts") {
      return(NULL)
    }
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getCohortCountResult(
      dataSource = dataSource,
      databaseIds = databaseIds(),
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
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    
    data <- getCohortCountResultReactive()
    validate(need(hasData(data), "There is no data on any cohort"))
    
    data <- getCohortCountResultReactive() %>%
      dplyr::rename(cohort = .data$shortName) %>%
      dplyr::rename(persons = .data$cohortSubjects,
                    records = .data$cohortEntries)
    
    dataColumnFields <- c("persons", "records")
    
    if (input$cohortCountsTableColumnFilter == "Persons") {
      dataColumnFields <- "persons"
    } else if (input$cohortCountsTableColumnFilter == "Records") {
      dataColumnFields <- "records"
    }
    
    keyColumnFields <- c("cohortId", "cohort")
    
    countsForHeader <- NULL
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
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
      selection = 'single'
    )
    return(displayTable)
  })
  
  getCohortIdOnCohortCountRowSelect <- reactive({
    idx <- reactable::getReactableState("cohortCountsTable", "selected")
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- getCohortCountResultReactive() %>% 
        dplyr::select(.data$cohortId, 
                      .data$shortName) %>% 
        dplyr::distinct()
      subset <- subset[idx,]
      return(subset)
    }
    
  })
  
  output$cohortCountRowIsSelected <- reactive({
    return(!is.null(getCohortIdOnCohortCountRowSelect()))
  })
  
  outputOptions(output,
                "cohortCountRowIsSelected",
                suspendWhenHidden = FALSE)
  
  output$InclusionRuleStatForCohortSeletedTable <- reactable::renderReactable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(
      nrow(getCohortIdOnCohortCountRowSelect()) > 0,
      "No cohorts chosen"
    ))
    data <- getInclusionRuleStats(
      dataSource = dataSource,
      cohortIds = getCohortIdOnCohortCountRowSelect()$cohortId,
      databaseIds = databaseIds()
    ) %>% dplyr::rename(Meet = .data$meetSubjects,
                        Gain = .data$gainSubjects,
                        Remain = .data$remainSubjects,
                        Total = .data$totalSubjects)
    
    countLocation <- 1
    keyColumnFields <-
      c("cohortId", "ruleName")
    dataColumnFields <- c("Meet","Gain","Remain","Total")
    
    validate(need((nrow(data) > 0),
                  "There is no data for the selected combination."))
    
    
    
    countsForHeader <- NULL
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
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
      selection = 'single'
    )
  })
  
  # Incidence rate --------------------------------------------------------------------------------
  
  incidenceRateData <- reactive({
    if (!exists('incidenceRate')) {
      return(NULL)
    }
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Sex" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    if (length(cohortIds()) > 0) {
      data <- getIncidenceRateResult(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        databaseIds = databaseIds(),
        stratifyByGender =  stratifyByGender,
        stratifyByAgeGroup =  stratifyByAge,
        stratifyByCalendarYear =  stratifyByCalendarYear,
        minPersonYears = input$minPersonYear,
        minSubjectCount = input$minSubjetCount
      ) %>%
        dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0,
                                                       TRUE ~ .data$incidenceRate))
      
    } else {
      data <- NULL
    }
    return(data)
  })
  
  shiny::observe({
    if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
      ageFilter <- incidenceRateData() %>%
        dplyr::select(.data$ageGroup) %>%
        dplyr::filter(.data$ageGroup != "NA", !is.na(.data$ageGroup)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(as.integer(sub(
          pattern = '-.+$', '', x = .data$ageGroup
        )))
      
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateAgeFilter",
        selected = ageFilter$ageGroup,
        choices = ageFilter$ageGroup,
        choicesOpt = list(style = rep_len("color: black;", 999))
      )
    }
  })
  
  shiny::observe({
    if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
      genderFilter <- incidenceRateData() %>%
        dplyr::select(.data$gender) %>%
        dplyr::filter(.data$gender != "NA",
                      !is.na(.data$gender)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$gender)
      
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateGenderFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = genderFilter$gender,
        selected = genderFilter$gender
      )
    }
  })
  
  shiny::observe({
    if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
      calenderFilter <- incidenceRateData() %>%
        dplyr::select(.data$calendarYear) %>%
        dplyr::filter(.data$calendarYear != "NA",
                      !is.na(.data$calendarYear)) %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear)
      
      minValue <- min(calenderFilter$calendarYear)
      
      maxValue <- max(calenderFilter$calendarYear)
      
      shiny::updateSliderInput(
        session = session,
        inputId = "incidenceRateCalenderFilter",
        min = minValue,
        max = maxValue,
        value = c(2010, maxValue)
      )
      
      minIncidenceRateValue <- round(min(incidenceRateData()$incidenceRate),digits = 2)
      
      maxIncidenceRateValue <- round(max(incidenceRateData()$incidenceRate),digits = 2)
      
      shiny::updateSliderInput(
        session = session,
        inputId = "YscaleMinAndMax",
        min = 0,
        max = maxIncidenceRateValue,
        value = c(minIncidenceRateValue, maxIncidenceRateValue),
        step = round((maxIncidenceRateValue - minIncidenceRateValue)/5,digits = 2)
      )
      
      
    }
  })
  
  incidenceRateAgeFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$incidenceRateAgeFilter_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$incidenceRateAgeFilter_open) ||
        !is.null(input$tabs)) {
      selectedIncidenceRateAgeFilter <- input$incidenceRateAgeFilter
      incidenceRateAgeFilter(selectedIncidenceRateAgeFilter)
    }
  })
  
  incidenceRateGenderFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$incidenceRateGenderFilter_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$incidenceRateGenderFilter_open) ||
        !is.null(input$tabs)) {
      selectedIncidenceRateGenderFilter <- input$incidenceRateGenderFilter
      incidenceRateGenderFilter(selectedIncidenceRateGenderFilter)
    }
  })
  
  incidenceRateCalenderFilter <- shiny::reactive({
    calenderFilter <- incidenceRateData() %>%
      dplyr::select(.data$calendarYear) %>%
      dplyr::filter(.data$calendarYear != "NA",
                    !is.na(.data$calendarYear)) %>%
      dplyr::distinct(.data$calendarYear) %>%
      dplyr::arrange(.data$calendarYear)
    calenderFilter <-
      calenderFilter[calenderFilter$calendarYear >= input$incidenceRateCalenderFilter[1] &
                       calenderFilter$calendarYear <= input$incidenceRateCalenderFilter[2], , drop = FALSE] %>%
      dplyr::pull(.data$calendarYear)
    return(calenderFilter)
  })
  
  
  incidenceRateYScaleFilter <- shiny::reactive({
    incidenceRateFilter <- incidenceRateData() %>%
      dplyr::select(.data$incidenceRate) %>%
      dplyr::filter(.data$incidenceRate != "NA",
                    !is.na(.data$incidenceRate)) %>%
      dplyr::distinct(.data$incidenceRate) %>%
      dplyr::arrange(.data$incidenceRate)
    incidenceRateFilter <-
      incidenceRateFilter[incidenceRateFilter$incidenceRate >= input$YscaleMinAndMax[1] &
                            incidenceRateFilter$incidenceRate <= input$YscaleMinAndMax[2], , drop = FALSE] %>%
      dplyr::pull(.data$incidenceRate)
    return(incidenceRateFilter)
  })
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Sex" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    shiny::withProgress(
      message = paste(
        "Building incidence rate plot data for ",
        length(cohortIds()),
        " cohorts and ",
        length(databaseIds()),
        " databases"
      ),{
        data <- incidenceRateData()
        
        validate(need(all(!is.null(data), nrow(data) > 0), paste0("No data for this combination")))
        
        if (stratifyByAge && !"All" %in% incidenceRateAgeFilter()) {
          data <- data %>%
            dplyr::filter(.data$ageGroup %in% incidenceRateAgeFilter())
        }
        if (stratifyByGender &&
            !"All" %in% incidenceRateGenderFilter()) {
          data <- data %>%
            dplyr::filter(.data$gender %in% incidenceRateGenderFilter())
        }
        if (stratifyByCalendarYear) {
          data <- data %>%
            dplyr::filter(.data$calendarYear %in% incidenceRateCalenderFilter())
        }
        if (input$irYscaleFixed) {
          data <- data %>%
            dplyr::filter(.data$incidenceRate %in% incidenceRateYScaleFilter())
        }
        if (all(!is.null(data), nrow(data) > 0)) {
          plot <- plotIncidenceRate(
            data = data,
            shortNameRef = cohort,
            stratifyByAgeGroup = stratifyByAge,
            stratifyByGender = stratifyByGender,
            stratifyByCalendarYear = stratifyByCalendarYear,
            yscaleFixed = input$irYscaleFixed
          )
          return(plot)
        }
        
      },detail = "Please Wait"
    )
  })
  
  # Time distribution -----------------------------------------------------------------------------
  timeDist <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getTimeDistributionResult(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = databaseIds()
    )
    return(data)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    data <- timeDist()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    plot <- plotTimeDistribution(data = data, shortNameRef = cohort)
    return(plot)
  })
  
  output$timeDistTable <- reactable::renderReactable(expr = {
    data <- timeDist()  %>%
      addShortName(cohort) %>%
      dplyr::arrange(.data$databaseId, .data$cohortId) %>%
      dplyr::mutate(
        # shortName = as.factor(.data$shortName),
        databaseId = as.factor(.data$databaseId)
      ) %>%
      dplyr::select(
        Database = .data$databaseId,
        Cohort = .data$shortName,
        TimeMeasure = .data$timeMetric,
        Average = .data$averageValue,
        SD = .data$standardDeviation,
        Min = .data$minValue,
        P10 = .data$p10Value,
        P25 = .data$p25Value,
        Median = .data$medianValue,
        P75 = .data$p75Value,
        P90 = .data$p90Value,
        Max = .data$maxValue
      )
    
    
    validate(need(all(!is.null(data), nrow(data) > 0),
                  "No data available for selected combination."))
    
    
    keyColumns <- c(
      "Database",
      "Cohort",
      "TimeMeasure"
    )
    
    dataColumns <- c("Average",
                     "SD",
                     "Min",
                     "P10",
                     "P25",
                     "Median",
                     "P75",
                     "P90",
                     "Max")
    
    getDisplayTableSimple(data = data,
                          keyColumns = keyColumns,
                          dataColumns = dataColumns)
  })
  
  # Concepts in data source-----------------------------------------------------------
  conceptsInDataSourceReactive <- shiny::reactive(x = {
    if (!input$tabs == "conceptsInDataSource") {
      return(NULL)
    }
    validate(need(all(!is.null(databaseIds()), length(databaseIds()) > 0), 
                  "No data sources chosen"))
    validate(need(all(!is.null(cohortId()),length(cohortId()) > 0),
                  "No cohort chosen"))
    data <- getConceptsInCohort(
      dataSource = dataSource,
      cohortId = cohortId(),
      databaseIds = databaseIds()
    )
    return(data)
  })
  
  output$conceptsInDataSourceTable <- reactable::renderReactable(expr = {
    validate(need(hasData(databaseIds()), "No cohort chosen"))
    validate(need(hasData(cohortId()), "No cohort chosen"))
    
    data <- conceptsInDataSourceReactive()
    validate(need(hasData(data),
                  "No data available for selected combination"
    ))
    if (hasData(input$conceptSetsSelected)) {
      if (length(getResolvedAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>%
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      }
    }
    validate(need(hasData(data),
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
    
    validate(need(hasData(data),"No data available for selected combination"))
    data <- data %>% 
      dplyr::rename(persons = .data$conceptSubjects,
                    records = .data$conceptCount) %>% 
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
        dataSource =  dataSource,
        databaseIds = databaseIds(),
        cohortIds = cohortId(),
        source = "cohort",
        fields = input$conceptsInDataSourceTableColumnFilter
      )
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
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
      showDataAsPercent =  showDataAsPercent,
      sort = TRUE
    )
    return(displayTable)
  })
  
  
  
  # Orphan concepts table -------------------------------------------------------------------------
  orphanConceptsDataReactive <- shiny::reactive(x = {
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    data <- getOrphanConceptResult(
      dataSource = dataSource,
      cohortId = cohortId(),
      databaseIds = database$databaseId
    )
    return(data)
  })
  
  output$orphanConceptsTable <- reactable::renderReactable(expr = {
    data <- orphanConceptsDataReactive()
    validate(need(hasData(data), "There is no data for the selected combination."))
    data <- data %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
      dplyr::filter(.data$cohortId %in% cohortId())
    validate(need(hasData(data), "There is no data for the selected combination."))
    
    if (hasData(input$conceptSetsSelected)) {
      if (!is.null(input$conceptSetsSelected)) {
        if (length(conceptSetIds()) > 0) {
          data <- data %>%
            dplyr::filter(.data$conceptSetId %in% conceptSetIds())
        } else {
          data <- data[0, ]
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
      dplyr::select(.data$databaseId,
                    .data$cohortId,
                    .data$conceptId,
                    .data$conceptSubjects,
                    .data$conceptCount) %>%
      dplyr::group_by(.data$databaseId,
                      .data$cohortId,
                      .data$conceptId) %>%
      dplyr::summarise(
        conceptSubjects = sum(.data$conceptSubjects),
        conceptCount = sum(.data$conceptCount),
        .groups = "keep"
      ) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(.data$databaseId) %>%
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
      dplyr::rename(persons = .data$conceptSubjects,
                    records = .data$conceptCount) %>%
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
        dataSource =  dataSource,
        databaseIds = data$databaseId %>% unique(),
        cohortIds = data$cohortId %>% unique(),
        source = "cohort",
        fields = input$orphanConceptsColumFilterType
      )
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
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
      showDataAsPercent =  showDataAsPercent,
      sort = TRUE
    )
    return(displayTable)
  })
  
  # Inclusion rules table -----------------------------------------------------------------------
  output$inclusionRuleTable <- reactable::renderReactable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    table <- getInclusionRuleStats(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds()
    ) %>% 
      dplyr::rename(Meet = .data$meetSubjects,
                    Gain = .data$gainSubjects,
                    Remain = .data$remainSubjects,
                    Total = .data$totalSubjects)
    
    validate(need((nrow(table) > 0),
                  "There is no data for the selected combination."))
    
    keyColumnFields <-
      c("cohortId", "ruleName")
    countLocation <- 1
    if (input$inclusionRuleTableFilters == "All") {
      dataColumnFields <- c("Meet","Gain","Remain","Total")
    } else {
      dataColumnFields <- input$inclusionRuleTableFilters
    }
    
    countsForHeader <-
      getDisplayTableHeaderCount(
        dataSource =  dataSource,
        databaseIds = databaseIds(),
        cohortIds = cohortId(),
        source = "cohort",
        fields = "Persons"
      )
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = table,
                                                    string = dataColumnFields)
    
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
      showDataAsPercent =  showDataAsPercent,
      sort = TRUE
    )
  })
  
  # Index event breakdown ----------------------------------------------------------------
  
  indexEventBreakDownData <- shiny::reactive(x = {
    if (length(cohortId()) > 0 &&
        length(databaseIds()) > 0) {
      data <- getIndexEventBreakdown(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = databaseIds(), 
        daysRelativeIndex = 0
      )
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      }
      if (!is.null(data)) {
        if (!'domainTable' %in% colnames(data)) {
          data$domainTable <- "Not in data"
        }
        if (!'domainField' %in% colnames(data)) {
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
        if (input$indexEventBreakdownTableRadioButton == 'All') {
          return(data)
        } else if (input$indexEventBreakdownTableRadioButton == "Standard concepts") {
          return(data %>% dplyr::filter(.data$standardConcept == 'S'))
        } else {
          return(data %>% dplyr::filter(is.na(.data$standardConcept)))
        }
      } else {
        return(NULL)
      }
    })
  
  domaintable <- shiny::reactive(x = {
    if (!is.null(indexEventBreakDownDataFilteredByRadioButton())) {
      return(
        indexEventBreakDownDataFilteredByRadioButton() %>%
          dplyr::pull(.data$domainTable) %>% unique()
      )
    } else {
      return(NULL)
    }
  })
  
  shiny::observe({
    data <- domaintable()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "breakdownDomainTable",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = data,
      selected = data
    )
  })
  
  shiny::observe({
    data <- indexEventBreakDownDataFilteredByRadioButton()
    if (!is.null(data) &&
        nrow(data) > 0) {
      data <- data %>%
        dplyr::filter(.data$domainTable %in% input$breakdownDomainTable) %>%
        dplyr::pull(.data$domainField) %>% unique()
    }
    
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "breakdownDomainField",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = data,
      selected = data
    )
  })
  
  selectedDomainTable <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$breakdownDomainTable_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$breakdownDomainTable_open) ||
        !is.null(input$tabs)) {
      selectedDomainTable(input$breakdownDomainTable)
    }
  })
  
  selectedDomainField <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$breakdownDomainField_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$breakdownDomainField_open) ||
        !is.null(input$tabs)) {
      selectedDomainField(input$breakdownDomainField)
    }
  })
  
  output$breakdownTable <- reactable::renderReactable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen chosen"))
    data <- indexEventBreakDownDataFilteredByRadioButton()
    
    validate(need(all(!is.null(data),nrow(data) > 0),
                  "There is no data for the selected combination."))
    
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
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
        .data$subjectCount
      ) %>%
      dplyr::filter(.data$conceptId > 0) %>%
      dplyr::distinct() %>%
      dplyr::rename(persons = .data$subjectCount,
                    records = .data$conceptCount) %>%
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
        dataSource =  dataSource,
        databaseIds = databaseIds(),
        cohortIds = cohortId(),
        source = "cohort",
        fields = input$indexEventBreakdownTableFilter
      )
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)   
    
    showDataAsPercent <- FALSE
    ## showDataAsPercent set based on UI selection - proportion
    
    getDisplayTableGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showDataAsPercent =  showDataAsPercent,
      sort = TRUE
    )
  })
  
  # Visit Context ---------------------------------------------------------------------------------------------
  getVisitContextData <- shiny::reactive(x = {
    if (all(hasData(input$tab),
            input$tab != "visitContext")) {
      return(NULL)
    }
    if (!hasData(databaseIds())) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"), !exists('visitContext'))) {
      return(NULL)
    }
    visitContext <-
      getVisitContextResults(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = databaseIds()
      )
    if (!hasData(visitContext)) {
      return(NULL)
    }
    return(visitContext)
  })
  
  ##getVisitContexDataEnhanced----
  getVisitContexDataEnhanced <- shiny::reactive(x = {
    if (input$tabs != "visitContext") {
      return(NULL)
    }
    
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
      dplyr::rename(subjects = .data$cohortSubjects,
                    records = .data$cohortEntries) %>% 
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
      tidyr::pivot_wider(id_cols = c("databaseId", "visitConceptName"), 
                         names_from = "visitContext", 
                         values_from = c("visitContextSubject"))
    return(visitContextData)
  })
  
  output$visitContextTable <- reactable::renderReactable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    data <- getVisitContexDataEnhanced()
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
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
        databaseIds = databaseIds(),
        cohortIds = cohortId(),
        source = "cohort",
        fields = input$visitContextPersonOrRecords
      )
    if (!hasData(countsForHeader)) {
      return(NULL)
    }
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
    getDisplayTableGroupedByDatabaseId(
      data = data,
      cohort = cohort, 
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = 1,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      sort = TRUE
    )
  })
  
  # Characterization -------------------------------------------------
  getConceptSetNameForFilter <- shiny::reactive(x = {
    if (length(cohortId()) == 0 || length(databaseIds()) == 0) {
      return(NULL)
    }
    
    jsonExpression <- cohortSubset() %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::select(.data$json)
    
    jsonExpression <- RJSONIO::fromJSON(jsonExpression$json, digits = 23)
    expression <-
      getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = jsonExpression)
    
    if (!is.null(expression)) {
      expression <- expression$conceptSetExpression %>% 
        dplyr::select(.data$name)
      
      return(expression)
    } else {
      return(NULL)
    }
  })
  
  characterizationDomainNameFilter <- shiny::reactive({
    return(input$characterizationDomainNameFilter)
  })
  
  characterizationAnalysisNameFilter <- shiny::reactive({
    return(input$characterizationAnalysisNameFilter)
  })
  
  characterizationTableData <- shiny::reactive(x = {
    if (!input$tabs %in% c("cohortCharacterization")) {
      return(NULL)
    }
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    if (input$charType == "Pretty") {
      analysisIds <- prettyAnalysisIds
    } else {
      analysisIds <- NULL
    }
    data <- getCovariateValueResult(
      dataSource = dataSource,
      analysisIds = analysisIds,
      cohortIds = cohortId(),
      databaseIds = databaseIds(),
      isTemporal = FALSE
    )
    
    if (hasData(data)) {
      if (input$charType == "Raw" &&
          input$charProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else if (input$charType == "Raw" &&
                 input$charProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'N')
      }
      return(data)
    } else {
      return(NULL)
    }
  })
  
  shiny::observe({
    subset <-
      characterizationTableData()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <-
      characterizationTableData()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <- getConceptSetNameForFilter()$name %>% sort() %>% unique()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "conceptSetsSelected",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  ## characterizationTableRawDataNotFiltered ----
  characterizationTableRawDataNotFiltered <- shiny::reactive(x = {
    data <- characterizationTableData()
    if (!hasData(data)) {
      return(NULL)
    } else {
      return(data)
    }
  })
  
  ## characterizationTableRawDataFiltered ----
  characterizationTableRawDataFiltered <- shiny::reactive(x = {
    data <- characterizationTableData()
    if (!hasData(data)) {
      return(NULL)
    }
    if (hasData(characterizationAnalysisNameFilter())) {
      data <- data %>%
        dplyr::filter(.data$analysisName %in% characterizationAnalysisNameFilter())
    }
    if (!hasData(data)) {
      return(NULL)
    } 
    if (hasData(characterizationAnalysisNameFilter())) {
      data <- data %>%
        dplyr::filter(.data$domainId %in% characterizationDomainNameFilter())
    }
    if (!hasData(data)) {
      return(NULL)
    }
    if (hasData(input$conceptSetsSelected)) {
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
  
  output$characterizationTable <- reactable::renderReactable(expr = {
    if (input$charType == "Pretty") {
      data <- characterizationTableRawDataNotFiltered()
      validate(need(hasData(data), "No data available for selected combination"))
      table <- data %>%
        prepareTable1()
      
      validate(need(nrow(table) > 0,
                    "No data available for selected combination."))
      
      characteristics <- table %>%
        dplyr::select(.data$characteristic,
                      .data$position,
                      .data$header,
                      .data$sortOrder) %>%
        dplyr::distinct() %>%
        dplyr::group_by(.data$characteristic, .data$position, .data$header) %>%
        dplyr::summarise(sortOrder = max(.data$sortOrder), .groups = "keep") %>%
        dplyr::ungroup() %>%
        dplyr::arrange(.data$position, desc(.data$header)) %>%
        dplyr::mutate(sortOrder = dplyr::row_number()) %>%
        dplyr::distinct()
      characteristics <- dplyr::bind_rows(
        characteristics %>%
          dplyr::filter(.data$header == 1) %>%
          dplyr::mutate(
            cohortId = sort(cohortId())[[1]],
            databaseId = sort(databaseIds()[[1]])
          ),
        characteristics %>%
          dplyr::filter(.data$header == 0) %>%
          tidyr::crossing(dplyr::tibble(databaseId = databaseIds())) %>%
          tidyr::crossing(dplyr::tibble(cohortId = cohortId()))
      ) %>%
        dplyr::arrange(.data$sortOrder, .data$databaseId, .data$cohortId)
      
      table <- characteristics %>%
        dplyr::left_join(
          table %>%
            dplyr::select(-.data$sortOrder),
          by = c(
            "characteristic",
            "position",
            "header",
            "databaseId",
            "cohortId"
          )
        ) 
      
      keyColumnFields <- c("characteristic")
      dataColumnFields <- c("value")
      
      showDataAsPercent <- TRUE ## showDataAsPercent set based on UI selection - proportion)
      
      countLocation <- 1
      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource =  dataSource,
          databaseIds = databaseIds(),
          cohortIds = cohortId(),
          source = "cohort",
          fields = "Persons"
        )
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = table,
                                                      string = dataColumnFields)
      displayTable <- getDisplayTableGroupedByDatabaseId(
        data = table,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent =  showDataAsPercent,
        sort = FALSE,
        pageSize = 100
      )
      return(displayTable)
    } else {
      data <- characterizationTableRawDataFiltered()
      validate(need(hasData(data), "No data available for selected combination"))
      data <- data %>% 
        dplyr::mutate(covariateName = paste(.data$covariateName,"(",.data$conceptId,")")) %>% 
        dplyr::select(.data$covariateName,
                      .data$mean,
                      .data$sd,
                      .data$cohortId,
                      .data$databaseId)
      
      keyColumnFields <- c("covariateName")
      
      showDataAsPercent <- FALSE
      if (input$characterizationColumnFilters == "Mean and Standard Deviation") {
        dataColumnFields <- c("mean","sd")
      } else {
        dataColumnFields <- c("mean")
        if (input$charProportionOrContinuous == "Proportion") {
          showDataAsPercent <- TRUE
        }
      }
      countLocation <- 1
      
      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource =  dataSource,
          databaseIds = databaseIds(),
          cohortIds = cohortId(),
          source = "cohort",
          fields = "Persons"
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent =  showDataAsPercent,
        sort = FALSE,
        pageSize = 100
      )
    }
  })
  
  # Temporal characterization -----------------------------------------------------------------
  temporalAnalysisNameFilter <- shiny::reactive(x = {
    return(input$temporalAnalysisNameFilter)
  })
  
  temporalDomainNameFilter <- shiny::reactive(x = {
    return(input$temporalDomainNameFilter)
  })
  
  temporalCharacterization <- shiny::reactive({
    if (!input$tabs %in% c("temporalCharacterization")) {
      return(NULL)
    }
    validate(need(length(timeIds()) > 0, "No time periods selected"))
    validate(need(length(input$database) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    data <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = input$database,
      timeIds = timeIds(),
      isTemporal = TRUE
    ) %>%
      dplyr::select(-.data$choices) %>% 
      dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>%
      dplyr::arrange(.data$timeId) %>%
      dplyr::select(-.data$cohortId, -.data$databaseId, -.data$covariateId)
    
    if (input$temporalProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$temporalProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    
    if (!is.null(input$conceptSetsSelected)) {
      if (length(getResolvedAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      } else {
        data <- data[0,]
      }
    }
    return(data)
  })
  
  shiny::observe({
    subset <-
      temporalCharacterization()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <-
      temporalCharacterization()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  ## Output -----------------------------------------------------------------------------
  output$temporalCharacterizationTable <-
    reactable::renderReactable(expr = {
      
      data <- temporalCharacterization()
      validate(need(hasData(data),
                    "No temporal characterization data"))
      data <- data %>%
        dplyr::filter(.data$analysisName %in% temporalAnalysisNameFilter()) %>%
        dplyr::filter(.data$domainId %in% temporalDomainNameFilter())
      
      temporalChoices <- data %>% 
        dplyr::distinct(.data$choices) %>% 
        dplyr::pull(.data$choices)
      
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      
      data <- data %>%
        dplyr::mutate(covariateName = paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
        tidyr::pivot_wider(
          id_cols = c("covariateName"),
          names_from = "choices",
          values_from = "mean" ,
          names_sep = "_"
        ) %>%
        dplyr::relocate(.data$covariateName) %>%
        dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with('Start'))))
      
      validate(need(all(!is.null(data), nrow(data) > 0),
                    "No data available for selected combination."))
      
      keyColumns <- c("covariateName")
      dataColumns <- c(temporalChoices)
      
      showDataAsPercent <- FALSE
      if (input$temporalProportionOrContinuous == "Proportion") {
        showDataAsPercent <- TRUE
      }
      
      getDisplayTableSimple(data = data,
                            keyColumns = keyColumns,
                            dataColumns = dataColumns,
                            showDataAsPercent = showDataAsPercent,
                            pageSize = 100)
    })
  
  #Cohort Overlap ------------------------
  cohortOverlapData <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 1, "Please select at least two cohorts."))
    combisOfTargetComparator <- t(utils::combn(cohortIds(), 2)) %>% 
      as.data.frame() %>% 
      dplyr::tibble()
    colnames(combisOfTargetComparator) <- c('targetCohortId', 'comparatorCohortId')
    
    data <- getCohortOverlapResult(
      dataSource = dataSource,
      targetCohortIds = combisOfTargetComparator$targetCohortId,
      comparatorCohortIds = combisOfTargetComparator$comparatorCohortId,
      databaseIds = databaseIds()
    )
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    validate(need(
      nrow(data) > 0,
      paste0("No cohort overlap data for this combination.")
    ))
    return(data)
  })
  
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length(cohortIds()) > 0,
      paste0("Please select Target Cohort(s)")
    ))
    
    data <- cohortOverlapData()
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    validate(need(
      nrow(data) > 0,
      paste0("No cohort overlap data for this combination.")
    ))
    
    plot <- plotCohortOverlap(
      data = data,
      shortNameRef = cohort,
      yAxis = input$overlapPlotType
    )
    return(plot)
  })
  
  # Compare cohort characteristics --------------------------------------------
  charCompareAnalysisNameFilter <- shiny::reactive(x = {
    return(input$charCompareAnalysisNameFilter)
  })
  
  charaCompareDomainNameFilter <- shiny::reactive(x = {
    return(input$charaCompareDomainNameFilter)
  })
  
  computeBalance <- shiny::reactive({
    if (!input$tabs %in% c("compareCohortCharacterization")) {
      return(NULL)
    }
    validate(need((length(cohortId(
    )) > 0), paste0("Please select cohort.")))
    validate(need((length(
      comparatorCohortId()
    ) > 0), paste0("Please select comparator cohort.")))
    validate(need((comparatorCohortId() != cohortId()),
                  paste0("Please select different cohorts for target and comparator cohorts.")
    ))
    validate(need((length(input$database) > 0),
                  paste0("Please select atleast one datasource.")
    ))
    
    covs1 <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = input$database,
      isTemporal = FALSE
    )
    
    covs2 <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = comparatorCohortId(),
      databaseIds = input$database,
      isTemporal = FALSE
    )
    
    if (is.null(covs1) || is.null(covs2)) {
      return(NULL)
    }
    
    balance <- compareCohortCharacteristics(covs1, covs2) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    
    if (input$charCompareType == "Raw table" &&
        input$charCompareProportionOrContinuous == "Proportion") {
      balance <- balance %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$charCompareType == "Raw table" &&
               input$charCompareProportionOrContinuous == "Continuous") {
      balance <- balance %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    return(balance)
  })
  
  shiny::observe({
    subset <- computeBalance()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "charCompareAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <- computeBalance()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "charaCompareDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  output$charCompareTable <- reactable::renderReactable(expr = {
    balance <- computeBalance()
    
    validate(need(all(!is.null(balance), nrow(balance) > 0),
                  "No data available for selected combination."))
    
    if (input$charCompareType == "Pretty table") {
      data <- prepareTable1Comp(balance)
      
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      
      data <- data %>%
        dplyr::arrange(.data$sortOrder) %>%
        dplyr::select(-.data$sortOrder) %>%
        dplyr::select(-.data$cohortId1, -.data$cohortId2)
      
      keyColumns <- c("characteristic")
      dataColumns <- c("MeanT","MeanC","StdDiff")
      
      getDisplayTableSimple(data = data,
                            keyColumns = keyColumns,
                            dataColumns = dataColumns)
    } else {
      balance <- balance %>%
        dplyr::filter(.data$analysisName %in% charCompareAnalysisNameFilter()) %>%
        dplyr::filter(.data$domainId %in% charaCompareDomainNameFilter())
      
      if (!is.null(input$conceptSetsSelected)) {
        balance <- balance %>% 
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      }
      
      validate(need(all(!is.null(balance), nrow(balance) > 0),
                    "No data available for selected combination."))
      
      data <- balance %>% 
        dplyr::mutate(covariateName =  paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
        dplyr::rename("meanTarget" = mean1,
                      "sdTarget" = sd1,
                      "meanComparator" = mean2,
                      "sdComparator" = sd2,
                      "StdDiff" = absStdDiff)
      
      keyColumnFields <- c("covariateName")
      showDataAsPercent <- FALSE
      if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation") {
        dataColumnFields <- c("meanTarget","sdTarget","meanComparator","sdComparator","StdDiff")
      } else {
        dataColumnFields <- c("meanTarget","meanComparator","StdDiff")
        if (input$charCompareProportionOrContinuous == "Proportion") {
          showDataAsPercent <- TRUE
        }
      }
      countLocation <- 1
      
      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource =  dataSource,
          databaseIds = databaseIds(),
          cohortIds = cohortId(),
          source = "cohort",
          fields = "Persons"
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent =  showDataAsPercent,
        sort = FALSE,
        pageSize = 100
      )
    }
  })
  
  output$charComparePlot <- ggiraph::renderggiraph(expr = {
    data <- computeBalance()
    
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    data <- data %>%
      dplyr::filter(.data$analysisName %in% charCompareAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% charaCompareDomainNameFilter())
    
    if (!is.null(input$conceptSetsSelected)) {
      if (length(getResolvedAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      } else {
        data <- data[0,]
      }
    }
    
    if (input$charCompareType == "Plot" &&
        input$charCompareProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$charCompareType == "Plot" &&
               input$charCompareProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    plot <-
      plotCohortComparisonStandardizedDifference(
        balance = data,
        shortNameRef = cohort,
        xLimitMin = 0,
        xLimitMax = 1,
        yLimitMin = 0,
        yLimitMax = 1
      )
    validate(need(!is.null(plot),
                  "No data available for selected combination."))
    return(plot)
  })
  
  #Compare Temporal Characterization.-----------------------------------------
  temporalCompareAnalysisNameFilter <- shiny::reactive(x = {
    return(input$temporalCompareAnalysisNameFilter)
  })
  
  temporalCompareDomainNameFilter <-  shiny::reactive(x = {
    return(input$temporalCompareDomainNameFilter)
  })
  
  computeBalanceForCompareTemporalCharacterization <-
    shiny::reactive({
      if (!input$tabs %in% c("compareTemporalCharacterization")) {
        return(NULL)
      }
      validate(need((length(cohortId(
      )) > 0),
      paste0("Please select cohort.")))
      validate(need((length(
        comparatorCohortId()
      ) > 0),
      paste0("Please select comparator cohort.")))
      validate(need((comparatorCohortId() != cohortId()),
                    paste0("Please select different cohorts for target and comparator cohorts.")
      ))
      validate(need((length(input$database) > 0),
                    paste0("Please select atleast one datasource.")
      ))
      validate(need((length(timeIds()) > 0), paste0("Please select time id")))
      covs1 <- getCovariateValueResult(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = input$database,
        isTemporal = TRUE,
        timeIds = timeIds()
      )
      validate(need((nrow(covs1) > 0), paste0("Target cohort id:", cohortId(), " does not have data.")))
      covs2 <- getCovariateValueResult(
        dataSource = dataSource,
        cohortIds = comparatorCohortId(),
        databaseIds = input$database,
        isTemporal = TRUE,
        timeIds = timeIds()
      )
      validate(need((nrow(covs2) > 0), paste0("Target cohort id:", comparatorCohortId(), " does not have data.")))
      
      balance <-
        compareTemporalCohortCharacteristics(covs1, covs2) %>%
        dplyr::mutate(absStdDiff = abs(.data$stdDiff))
      
      if (input$temporalCharacterizationType == "Raw table" &&
          input$temporalCharacterProportionOrContinuous == "Proportion") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else if (input$temporalCharacterizationType == "Raw table" &&
                 input$temporalCharacterProportionOrContinuous == "Continuous") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'N')
      }
      
      if (input$temporalCharacterizationType == "Plot" &&
          input$temporalCharacterProportionOrContinuous == "Proportion") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else if (input$temporalCharacterizationType == "Plot" &&
                 input$temporalCharacterProportionOrContinuous == "Continuous") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'N')
      }
      
      return(balance)
    })
  
  shiny::observe({
    subset <-
      computeBalanceForCompareTemporalCharacterization()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCompareAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <-
      computeBalanceForCompareTemporalCharacterization()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCompareDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  output$temporalCharacterizationCompareTable <-
    reactable::renderReactable(expr = {
      balance <- computeBalanceForCompareTemporalCharacterization()
      
      validate(need(nrow(balance) > 0,
                    "No data available for selected combination."))
      
      if (input$temporalCharacterizationType == "Raw table") {
        balance <- balance %>%
          dplyr::filter(.data$analysisName %in% temporalCompareAnalysisNameFilter()) %>%
          dplyr::filter(.data$domainId %in% temporalCompareDomainNameFilter())
        
        if (!is.null(input$conceptSetsSelected)) {
          balance <- balance %>% 
            dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
        }
        
        validate(need(all(!is.null(balance), nrow(balance) > 0),
                      "No data available for selected combination."))
        
        balance <- balance %>% 
          dplyr::rename("meanTarget" = mean1, 
                        "sDTarget" = sd1,
                        "meanComparator" = mean2,
                        "sDComparator" = sd2,
                        "stdDiff" = stdDiff)
        
        data <- balance %>%
          dplyr::mutate(covariateName = paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
          dplyr::arrange(desc(abs(.data$stdDiff))) 
        
        showDataAsPercent <- FALSE
        keyColumnFields <- c("covariateId","covariateName")
        if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation") {
          dataColumnFields <- c("meanTarget","sDTarget","meanComparator","sDComparator","stdDiff")
        } else {
          dataColumnFields <- c("meanTarget","meanComparator","stdDiff")
          
          if (input$temporalCharacterProportionOrContinuous == "Proportion") {
            showDataAsPercent <- TRUE
          }
        }
        
        maxCountValue <-
          getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                        string = dataColumnFields)
        
        getDisplayTableGroupedByDatabaseId(
          data = data,
          cohort = cohort,
          database = database,
          headerCount = NULL,
          keyColumns = keyColumnFields,
          countLocation = 1,
          dataColumns = dataColumnFields,
          maxCount = maxCountValue,
          showDataAsPercent =  showDataAsPercent,
          sort = TRUE,
          isTemporal = TRUE,
          pageSize = 100
        )
      }
    })
  
  output$temporalCharComparePlot <- ggiraph::renderggiraph(expr = {
    data <- computeBalanceForCompareTemporalCharacterization()
    validate(need(nrow(data) != 0, paste0("No data for the selected combination.")))
    
    data <- data %>%
      dplyr::filter(.data$analysisName %in% temporalCompareAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% temporalCompareDomainNameFilter()) 
    
    if (!is.null(input$conceptSetsSelected)) {
      if (length(getResolvedAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
      } else {
        data <- data[0,]
      }
    }
    
    validate(need(nrow(data) != 0, paste0("No data for the selected combination.")))
    
    validate(need((nrow(data) - nrow(data[data$mean1 < 0.001, ])) > 5 &&
                    (nrow(data) - nrow(data[data$mean2 < 0.001, ])) > 5, paste0("No data for the selected combination.")))
    
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
  
  output$databaseInformationTable <- reactable::renderReactable(expr = {
    if (!input$tabs == "databaseInformation") {
      return(NULL)
    }
    validate(need(all(!is.null(database), nrow(database) > 0),
                  "No data available for selected combination."))
    
    data <- database 
    if (!'vocabularyVersionCdm' %in% colnames(database)) {
      data$vocabularyVersionCdm <- "Not in data"
    }
    if (!'vocabularyVersion' %in% colnames(database)) {
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
        "cdmReleaseDate" ,
        "observationPeriodMinDate",
        "observationPeriodMaxDate"
      )
    )
    
    dataColumns <- c("personsInDatasource",
                     "recordsInDatasource",
                     "personDaysInDatasource")
    
    data <- getDisplayTableSimple(data = data,
                                  keyColumns = keyColumns,
                                  dataColumns = dataColumns)
    return(data)
  })
  
  #Login User ---------------------------------------------
  activeLoggedInUser <- reactiveVal(NULL)
  
  if (enableAnnotation && exists("userCredentials") && nrow(userCredentials) > 0) {
    shiny::observeEvent(eventExpr = input$annotationUserPopUp,
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
                                  value = if (enableAuthorization) "" else {"annonymous"}
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
                        })
    
    
    shiny::observeEvent(eventExpr = input$login,
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
                        })
    
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
    
    #Annotation Section ------------------------------------
    ## Annotation enabled ------
    output$postAnnotationEnabled <- shiny::reactive({
      return(!is.null(activeLoggedInUser()) & enableAnnotation)
    })
    shiny::outputOptions(x = output,
                         name = "postAnnotationEnabled",
                         suspendWhenHidden = FALSE)
    
    ## Retrieve Annotation ----------------
    reloadAnnotationSection <- reactiveVal(0)
    getAnnotationReactive <- shiny::reactive({
      reloadAnnotationSection()
      if (input$tabs == 'cohortCounts' |
          input$tabs == 'cohortOverlap' |
          input$tabs == 'incidenceRate' |
          input$tabs == 'timeDistribution') {
        selectedCohortIds <- cohort %>%
          dplyr::filter(.data$compoundName %in% c(input$cohorts)) %>%  #many cohorts selected
          dplyr::pull(.data$cohortId)
      } else {
        selectedCohortIds <- cohort %>%
          dplyr::filter(.data$compoundName %in% c(input$cohort)) %>%  #one cohort selected
          dplyr::pull(.data$cohortId)
      }
      results <- getAnnotationResult(
        dataSource = dataSource,
        diagnosticsId = input$tabs,
        cohortIds = selectedCohortIds,
        databaseIds = databaseIds()
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
                  dplyr::filter(.data$annotationId == data[index, ]$annotationId) %>%
                  dplyr::inner_join(cohort %>%
                                      dplyr::select(.data$cohortId,
                                                    .data$cohortName),
                                    by = "cohortId")
                distinctCohortName <- subTable %>%
                  dplyr::distinct(.data$cohortName)
                distinctDatabaseId <-  subTable %>%
                  dplyr::distinct(.data$databaseId)
                
                htmltools::div(
                  style = "margin:0;padding:0;padding-left:50px;",
                  tags$p(
                    style = "margin:0;padding:0;",
                    "Related Cohorts: ",
                    tags$p(style = "padding-left:30px;",
                           tags$pre(
                             paste(distinctCohortName$cohortName, collapse = "\n")
                           ))
                  ),
                  tags$br(),
                  tags$p(
                    "Related Databses: ",
                    tags$p(style = "padding-left:30px;",
                           tags$pre(
                             paste(distinctDatabaseId$databaseId, collapse = "\n")
                           ))
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
      
      #Annotation - cohort Ids
      if (!is.null(input[[paste0("cohort", input$tabs)]])) {
        selectedCohortIds <-
          cohort %>%
          dplyr::filter(.data$compoundName %in% input[[paste0("cohort", input$tabs)]]) %>%
          dplyr::pull(.data$cohortId)
        #cohortsConceptInDataSource should be the same as in menu cohort
      } else {
        selectedCohortIds <- input$cohort
      }
      tempList$cohortIds <- selectedCohortIds
      
      #Annotation - database Ids
      if (!is.null(input[[paste0("database", input$tabs)]])) {
        selectedDatabaseIds <- input[[paste0("database", input$tabs)]]
      } else {
        selectedDatabaseIds <- input$databases
      }
      tempList$databaseIds <- selectedDatabaseIds
      return(tempList)
    })
    
    postAnnotationTabList <- reactiveVal(c())
    observeEvent(eventExpr = input[[paste0("postAnnotation", input$tabs)]], {
      if (!paste(toString(input[[paste0("postAnnotation", input$tabs)]]), input$tabs) %in% postAnnotationTabList()) {
        postAnnotationTabList(c(postAnnotationTabList(),
                                paste(toString(input[[paste0("postAnnotation", input$tabs)]]), input$tabs)))
      }
    })
    
    observeEvent(eventExpr = postAnnotationTabList(),
                 handlerExpr = {
                   parametersToPostAnnotation <- getParametersToPostAnnotation()
                   annotation <-
                     renderedAnnotation()()   #()() - This is to retrieve a function inside reactive
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
                     #trigger reload
                     reloadAnnotationSection(reloadAnnotationSection() + 1)
                   }
                 })
  }
  # Infoboxes ------------------------------------------------------------------------
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
    showInfoBox("Concepts in data source",
                "html/conceptsInDataSource.html")
  })
  
  shiny::observeEvent(input$orphanConceptsInfo, {
    showInfoBox("Orphan (Source) Concepts", "html/orphanConcepts.html")
  })
  
  shiny::observeEvent(input$conceptSetDiagnosticsInfo, {
    showInfoBox("Concept Set Diagnostics",
                "html/conceptSetDiagnostics.html")
  })
  
  shiny::observeEvent(input$inclusionRuleStatsInfo, {
    showInfoBox("Inclusion Rule Statistics",
                "html/inclusionRuleStats.html")
  })
  
  shiny::observeEvent(input$indexEventBreakdownInfo, {
    showInfoBox("Index Event Breakdown", "html/indexEventBreakdown.html")
  })
  
  shiny::observeEvent(input$visitContextInfo, {
    showInfoBox("Visit Context", "html/visitContext.html")
  })
  
  shiny::observeEvent(input$cohortCharacterizationInfo, {
    showInfoBox("Cohort Characterization",
                "html/cohortCharacterization.html")
  })
  
  shiny::observeEvent(input$temporalCharacterizationInfo, {
    showInfoBox("Temporal Characterization",
                "html/temporalCharacterization.html")
  })
  
  shiny::observeEvent(input$cohortOverlapInfo, {
    showInfoBox("Cohort Overlap", "html/cohortOverlap.html")
  })
  
  shiny::observeEvent(input$compareCohortCharacterizationInfo, {
    showInfoBox("Compare Cohort Characteristics",
                "html/compareCohortCharacterization.html")
  })
  
  # Cohort labels --------------------------------------------------------------------------------------------
  targetCohortCount <- shiny::reactive({
    targetCohortWithCount <-
      getCohortCountResult(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = input$database
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
    return(apply(cohorts, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  selectedCohort <- shiny::reactive({
    return(input$cohort)
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
  output$conceptsInDataSourceSelectedCohort <-
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
  output$cohortOverlapSelectedCohort <-
    shiny::renderUI({
      selectedCohorts()
    })
  output$incidenceRateSelectedCohorts <-
    shiny::renderUI({
      selectedCohorts()
    })
  output$timeDistSelectedCohorts <-
    shiny::renderUI({
      selectedCohorts()
    })
  output$visitContextSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$temporalCharacterizationSelectedCohort <-
    shiny::renderUI({
      return(selectedCohort())
    })
  
  output$temporalCharacterizationSelectedDatabase <-
    shiny::renderUI({
      return(input$database)
    })
  
  output$cohortCharCompareSelectedCohort <- shiny::renderUI({
    htmltools::withTags(table(tr(td(
      selectedCohort()
    )),
    tr(td(
      selectedComparatorCohort()
    ))))
  })
  
  output$cohortCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(input$database)
    })
  
  output$temporalCharCompareSelectedCohort <-
    shiny::renderUI({
      htmltools::withTags(table(tr(td(
        selectedCohort()
      )),
      tr(td(
        selectedComparatorCohort()
      ))))
    })
  
  output$temporalCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(input$database)
    })
})
