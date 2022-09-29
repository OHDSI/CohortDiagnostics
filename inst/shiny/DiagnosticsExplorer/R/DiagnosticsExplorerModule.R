diagnosticsExplorerModule <- function(id = "DiagnosticsExplorer",
                                      envir = .GlobalEnv,
                                      dataSource = envir$dataSource,
                                      databaseTable = envir$database,
                                      cohortTable = envir$cohort,
                                      cohortCountTable = envir$cohortCount,
                                      enableAnnotation = envir$enableAnnotation,
                                      enableAuthorization = envir$enableAuthorization,
                                      enabledTabs = envir$enabledTabs,
                                      conceptSets = envir$conceptSets,
                                      userCredentials = envir$userCredentials,
                                      activeUser = envir$activeUser) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {

    activeLoggedInUser <- reactiveVal(activeUser)
    if (enableAnnotation & nrow(userCredentials) > 0) {
      shiny::observeEvent(
        eventExpr = input$annotationUserPopUp,
        handlerExpr = {
          shiny::showModal(
            shiny::modalDialog(
              title = "Annotate",
              easyClose = TRUE,
              size = "s",
              footer = tagList(
                shiny::actionButton(inputId = ns("login"), label = "Login"),
                shiny::modalButton("Cancel")
              ),
              tags$div(
                shiny::textInput(
                  inputId = ns("userName"),
                  label = "Username",
                  width = NULL,
                  value = if (enableAuthorization) {
                    ""
                  } else {
                    "annonymous"
                  }
                ),
                if (enableAuthorization) {
                  shiny::passwordInput(
                    inputId = ns("password"),
                    label = "Password",
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
              if (enableAuthorization) {
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
    }

    output$userNameLabel <- shiny::renderText({
      if (is.null(activeLoggedInUser())) {
        return("")
      }
      paste(as.character(icon("user")),
            stringr::str_to_title(activeLoggedInUser()))

    })

    # Display login based on value of active logged in user
    postAnnotaionEnabled <- shiny::reactive(!is.null(activeLoggedInUser()))
    output$postAnnoataionEnabled <- shiny::reactive({
      postAnnotaionEnabled()
    })

    output$signInButton <- shiny::renderUI({
      if (enableAuthorization & !postAnnotaionEnabled()) {
        return(
          shiny::actionButton(
            inputId = ns("annotationUserPopUp"),
            label = "Sign in"
          )
        )
      } else {
        return(shiny::span())
      }
    })

    outputOptions(output, "postAnnoataionEnabled", suspendWhenHidden = FALSE)

    if (enableAnnotation) {
      #--- Annotation modules
      annotationModules <- c("cohortCountsAnnotation",
                             "timeDistributionAnnotation",
                             "conceptsInDataSourceAnnotation",
                             "orphanConceptsAnnotation",
                             "inclusionRuleStatsAnnotation",
                             "indexEventBreakdownAnnotation",
                             "visitContextAnnotation",
                             "cohortOverlapAnnotation",
                             "cohortCharacterizationAnnotation",
                             "temporalCharacterizationAnnotation",
                             "compareCohortCharacterizationAnnotation",
                             "compareTemporalCharacterizationAnnotation")


      for (module in annotationModules) {
        annotationModule(id = module,
                         dataSource = dataSource,
                         activeLoggedInUser = activeLoggedInUser,
                         selectedDatabaseIds = selectedDatabaseIds,
                         selectedCohortIds = inputCohortIds,
                         cohortTable = cohortTable,
                         databaseTable = databaseTable,
                         postAnnotaionEnabled = postAnnotaionEnabled)
      }
    }

    # Reacive: targetCohortId
    targetCohortId <- shiny::reactive({
      return(cohortTable$cohortId[cohortTable$compoundName == input$targetCohort])
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
          cohortTable$cohortId[cohortTable$compoundName %in% input$cohorts]
        cohortIds(selectedCohortIds)
      }
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

    databaseChoices <- databaseTable$databaseId
    names(databaseChoices) <- databaseTable$databaseName

    ## ReactiveValue: selectedDatabaseIds ----
    selectedDatabaseIds <- shiny::reactive({
      if (!is.null(input$tabs)) {
        if (input$tabs %in% c(
          "compareCohortCharacterization",
          "compareTemporalCharacterization",
          "temporalCharacterization",
          "databaseInformation"
        )) {
          return(input$database)
        } else {
          return(input$databases)
        }
      }
    })


    shiny::observe({
      shinyWidgets::updatePickerInput(session = session,
                                      inputId = "database",
                                      choices = databaseChoices,
                                      selected = databaseChoices[[1]],
      )
      shinyWidgets::updatePickerInput(session = session,
                                      inputId = "databases",
                                      choices = databaseChoices,
                                      selected = databaseChoices[[1]],
      )
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
        !is.null(input$tabs) & !is.null(envir$temporalCharacterizationTimeIdChoices)) {
        selectedTemporalTimeIds(
          envir$temporalCharacterizationTimeIdChoices %>%
            dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
            dplyr::pull(.data$timeId) %>%
            unique() %>%
            sort()
        )
      }
    })

    cohortSubset <- shiny::reactive({
      return(cohortTable %>%
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


    inputCohortIds <- shiny::reactive({
      if (input$tabs == "cohortCounts" |
        input$tabs == "cohortOverlap" |
        input$tabs == "incidenceRate" |
        input$tabs == "timeDistribution") {
        subset <- input$cohorts
      } else {
        subset <- input$targetCohort
      }

      return(subset)
    })

    shiny::observe({
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = paste0("targetCohort", input$tabs),
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = inputCohortIds(),
        selected = inputCohortIds()
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

    if ("cohort" %in% enabledTabs) {
      cohortDefinitionsModule(id = "cohortDefinitions",
                              dataSource = dataSource,
                              cohortDefinitions = cohortSubset,
                              cohortTable = cohortTable,
                              cohortCount = cohortCountTable,
                              databaseTable = databaseTable)
    }

    if ("includedSourceConcept" %in% enabledTabs) {
      conceptsInDataSourceModule(id = "conceptsInDataSource",
                                 dataSource = dataSource,
                                 selectedCohort = selectedCohort,
                                 selectedDatabaseIds = selectedDatabaseIds,
                                 targetCohortId = targetCohortId,
                                 selectedConceptSets = selectedConceptSets,
                                 cohortTable = cohortTable,
                                 databaseTable = databaseTable)
    }

    if ("orphanConcept" %in% enabledTabs) {
      orphanConceptsModule("orphanConcepts",
                           dataSource = dataSource,
                           selectedCohorts = selectedCohort,
                           selectedDatabaseIds = selectedDatabaseIds,
                           targetCohortId = targetCohortId,
                           selectedConceptSets = selectedConceptSets,
                           conceptSetIds = conceptSetIds)
    }

    if ("cohortCount" %in% enabledTabs) {
      cohortCountsModule(id = "cohortCounts",
                         dataSource = dataSource,
                         cohortTable = cohortTable, # The injection of tables like this should be removed
                         databaseTable = databaseTable, # The injection of tables like this should be removed
                         selectedCohorts = selectedCohorts,
                         selectedDatabaseIds = selectedDatabaseIds,
                         cohortIds = cohortIds)
    }

    if ("cohortIncStats" %in% enabledTabs) {
      inclusionRulesModule(id = "inclusionRules",
                           dataSource = dataSource,
                           cohortTable = cohortTable,
                           databaseTable = databaseTable,
                           selectedCohort = selectedCohort,
                           targetCohortId = targetCohortId,
                           selectedDatabaseIds = selectedDatabaseIds)
    }

    if ("indexEventBreakdown" %in% enabledTabs) {
      indexEventBreakdownModule("indexEvents",
                                dataSource = dataSource,
                                selectedCohort = selectedCohort,
                                targetCohortId = targetCohortId,
                                selectedDatabaseIds = selectedDatabaseIds)
    }

    if ("visitContext" %in% enabledTabs) {
      visitContextModule(id = "visitContext",
                         dataSource = dataSource,
                         selectedCohort = selectedCohort,
                         selectedDatabaseIds = selectedDatabaseIds,
                         targetCohortId = targetCohortId,
                         cohortTable = cohortTable,
                         databaseTable = databaseTable)
    }

    if ("relationship" %in% enabledTabs) {
      cohortOverlapModule(id = "cohortOverlap",
                          dataSource = dataSource,
                          selectedCohorts = selectedCohorts,
                          selectedDatabaseIds = selectedDatabaseIds,
                          targetCohortId = targetCohortId,
                          cohortIds = cohortIds,
                          cohortTable = cohortTable)
    }

    if ("temporalCovariateValue" %in% enabledTabs) {
      timeDistributionsModule(id = "timeDistributions",
                              dataSource = dataSource,
                              selectedCohorts = selectedCohorts,
                              cohortIds = cohortIds,
                              selectedDatabaseIds = selectedDatabaseIds,
                              cohortTable = cohortTable,
                              databaseTable = databaseTable)

      characterizationModule(id = "characterization",
                             dataSource = dataSource,
                             cohortTable = cohortTable,
                             databaseTable = databaseTable,
                             temporalAnalysisRef = envir$temporalAnalysisRef,
                             analysisNameOptions = envir$analysisNameOptions,
                             domainIdOptions = envir$domainIdOptions,
                             analysisIdInCohortCharacterization = envir$analysisIdInCohortCharacterization,
                             characterizationTimeIdChoices = envir$characterizationTimeIdChoices)

      compareCohortCharacterizationModule("compareCohortCharacterization",
                                          dataSource = dataSource,
                                          cohortTable = cohortTable,
                                          databaseTable = databaseTable,
                                          conceptSets = conceptSets,
                                          temporalAnalysisRef = envir$temporalAnalysisRef,
                                          analysisNameOptions = envir$analysisNameOptions,
                                          domainIdOptions = envir$domainIdOptions,
                                          temporalChoices = envir$temporalChoices)
    }

    if ("incidenceRate" %in% enabledTabs) {
      incidenceRatesModule(id = "incidenceRates",
                           dataSource = dataSource,
                           selectedCohorts = selectedCohorts,
                           cohortIds = cohortIds,
                           selectedDatabaseIds = selectedDatabaseIds,
                           cohortTable = cohortTable)
    }

    databaseInformationModule(id = "databaseInformation",
                              dataSource = dataSource,
                              selectedDatabaseIds = selectedDatabaseIds,
                              databaseMetadata = envir$databaseMetadata)

  })

}