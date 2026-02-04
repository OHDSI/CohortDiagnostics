# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of PatientLevelPrediction
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Global ranges for IR values
getIncidenceRateRanges <- function(dataSource, minPersonYears = 0) {
  sql <- "SELECT DISTINCT age_group FROM @schema.@ir_table WHERE person_years >= @person_years"

  ageGroups <- dataSource$connectionHandler$queryDb(
    sql = sql,
    schema = dataSource$schema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::mutate(ageGroup = dplyr::na_if(.data$ageGroup, ""))

  sql <- "SELECT DISTINCT calendar_year FROM @schema.@ir_table WHERE person_years >= @person_years"

  calendarYear <- dataSource$connectionHandler$queryDb(
    sql = sql,
    schema = dataSource$schema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::mutate(
      calendarYear = dplyr::na_if(.data$calendarYear, "")
    ) %>%
    dplyr::mutate(calendarYear = as.integer(.data$calendarYear))

  sql <- "SELECT DISTINCT gender FROM @schema.@ir_table WHERE person_years >= @person_years"

  gender <- dataSource$connectionHandler$queryDb(
    sql = sql,
    schema = dataSource$schema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::mutate(gender = dplyr::na_if(.data$gender, ""))


  sql <- "SELECT
    min(incidence_rate) as min_ir,
    max(incidence_rate) as max_ir
   FROM @schema.@ir_table
   WHERE person_years >= @person_years
   AND incidence_rate > 0.0
   "

  incidenceRate <- dataSource$connectionHandler$queryDb(
    sql = sql,
    schema = dataSource$schema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  )

  return(list(gender = gender,
              incidenceRate = incidenceRate,
              calendarYear = calendarYear,
              ageGroups = ageGroups))
}

getIncidenceRateResult <- function(dataSource,
                                   cohortIds,
                                   databaseIds,
                                   stratifyByGender = c(TRUE, FALSE),
                                   stratifyByAgeGroup = c(TRUE, FALSE),
                                   stratifyByCalendarYear = c(TRUE, FALSE),
                                   minPersonYears = 1000,
                                   minSubjectCount = NA) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <-
    checkErrorCohortIdsDatabaseIds(
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      errorMessage = errorMessage
    )
  checkmate::assertLogical(
    x = stratifyByGender,
    add = errorMessage,
    min.len = 1,
    max.len = 2,
    unique = TRUE
  )
  checkmate::assertLogical(
    x = stratifyByAgeGroup,
    add = errorMessage,
    min.len = 1,
    max.len = 2,
    unique = TRUE
  )
  checkmate::assertLogical(
    x = stratifyByCalendarYear,
    add = errorMessage,
    min.len = 1,
    max.len = 2,
    unique = TRUE
  )
  checkmate::reportAssertions(collection = errorMessage)

  sql <- "SELECT ir.*, cc.cohort_subjects
            FROM  @schema.@ir_table ir
            INNER JOIN @schema.@cc_table cc ON (
              ir.database_id = cc.database_id AND ir.cohort_id = cc.cohort_id
            )
            WHERE ir.cohort_id in (@cohort_ids)
           	  AND ir.database_id in (@database_ids)
            {@gender == TRUE} ? {AND ir.gender IS NOT NULL} : {  AND ir.gender IS NULL}
            {@age_group == TRUE} ? {AND ir.age_group IS NOT NULL} : {  AND ir.age_group IS NULL}
            {@calendar_year == TRUE} ? {AND ir.calendar_year IS NOT NULL} : {  AND ir.calendar_year IS NULL}
              AND ir.person_years > @personYears;"
  
  data <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      cohort_ids = cohortIds,
      database_ids = quoteLiterals(databaseIds),
      gender = stratifyByGender,
      age_group = stratifyByAgeGroup,
      calendar_year = stratifyByCalendarYear,
      personYears = minPersonYears,
      ir_table = dataSource$prefixTable("incidence_rate"),
      cc_table = dataSource$prefixTable("cohort_count"),
      #database_table = paste0(dataSource$databaseTablePrefix, dataSource$databaseTable),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()
  
  # join with dbTable (moved this outside sql)
  data <- merge(
    data, 
    dataSource$dbTable, 
    by = 'databaseId'
    )
  
  data <- tidyr::as_tibble(data)
  
  data <- data %>%
    dplyr::mutate(
      gender = dplyr::na_if(.data$gender, ""),
      ageGroup = dplyr::na_if(.data$ageGroup, ""),
      calendarYear = dplyr::na_if(.data$calendarYear, "")
    ) %>%
    dplyr::mutate(calendarYear = as.integer(.data$calendarYear)) %>%
    dplyr::arrange(.data$cohortId, .data$databaseId)


  if (!is.na(minSubjectCount)) {
    data <- data %>%
      dplyr::filter(.data$cohortSubjects > !!minSubjectCount)
  }

  return(data)
}

plotIncidenceRate <- function(data,
                              cohortTable = NULL,
                              stratifyByAgeGroup = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              yscaleFixed = FALSE,
                              yRange = c(0, 1.0)) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(
    x = data,
    any.missing = TRUE,
    min.rows = 1,
    min.cols = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByAgeGroup,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByGender,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByCalendarYear,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = yscaleFixed,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertDouble(
    x = data$incidenceRate,
    lower = 0,
    any.missing = FALSE,
    null.ok = FALSE,
    min.len = 1,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  checkmate::assertDouble(
    x = data$incidenceRate,
    lower = 0,
    any.missing = FALSE,
    null.ok = FALSE,
    min.len = 1,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)

  cohortNames <- cohortTable %>% dplyr::select("cohortId",
                                               "cohortName")
  plotData <- data %>%
    dplyr::inner_join(cohortNames, by = "cohortId",) %>%
    addShortName(cohortTable) %>%
    dplyr::mutate(incidenceRate = round(.data$incidenceRate, digits = 3))

  plotData <- plotData %>%
    dplyr::mutate(
      strataGender = !is.na(.data$gender),
      strataAgeGroup = !is.na(.data$ageGroup),
      strataCalendarYear = !is.na(.data$calendarYear)
    ) %>%
    dplyr::filter(
      .data$strataGender %in% !!stratifyByGender &
        .data$strataAgeGroup %in% !!stratifyByAgeGroup &
        .data$strataCalendarYear %in% !!stratifyByCalendarYear
    ) %>%
    dplyr::select(-dplyr::starts_with("strata"))

  sortShortName <- plotData %>%
    dplyr::select("shortName") %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = "^C", "", x = .data$shortName
    )))

  plotData <- plotData %>%
    dplyr::arrange(
      shortName = factor(.data$shortName, levels = sortShortName$shortName),
      .data$shortName
    )

  plotData$shortName <- factor(plotData$shortName,
                               levels = sortShortName$shortName)

  if (stratifyByAgeGroup) {
    sortAgeGroup <- plotData %>%
      dplyr::select("ageGroup") %>%
      dplyr::distinct() %>%
      dplyr::arrange(as.integer(sub(
        pattern = "-.+$", "", x = .data$ageGroup
      )))

    plotData <- plotData %>%
      dplyr::arrange(
        ageGroup = factor(.data$ageGroup, levels = sortAgeGroup$ageGroup),
        .data$ageGroup
      )

    plotData$ageGroup <- factor(plotData$ageGroup,
                                levels = sortAgeGroup$ageGroup
    )
  }

  plotData$tooltip <- c(
    paste0(
      plotData$cohortId, " ", plotData$cohortName,
      "\n",
      plotData$databaseName,
      "\nIncidence Rate = ",
      scales::comma(plotData$incidenceRate, accuracy = 0.01),
      "/per 1k PY",
      "\nIncidence Proportion = ",
      scales::percent(plotData$cohortCount / plotData$cohortSubjects, accuracy = 0.1),
      "\nPerson years = ",
      scales::comma(plotData$personYears, accuracy = 0.01),
      "\nCohort count = ",
      scales::comma(plotData$cohortSubjects, accuracy = 1),
      "\nCount = ",
      paste0(scales::comma(plotData$cohortCount, accuracy = 1))
    )
  )

  if (stratifyByAgeGroup) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nAge Group = ", plotData$ageGroup))
  }

  if (stratifyByGender) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nSex = ", plotData$gender))
  }

  if (stratifyByCalendarYear) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nYear = ", plotData$calendarYear))
  }

  if (stratifyByGender) {
    # Make sure colors are consistent, no matter which genders are included:
    genders <- c("Female", "Male", "No matching concept")
    # Code used to generate palette:
    colors <- RColorBrewer::brewer.pal(n = 3, name = "Dark2")
    plotData$gender <- factor(plotData$gender, levels = genders)
  } else {
    colors <- "#337ab7"
    plotData$gender <- ""
  }

  ncohorts <- plotData$shortName %>% unique() %>% length()
  ndatabases <- plotData$databaseName %>% unique() %>% length()
  nrows <- ncohorts * ndatabases

  subplots <- list()
  topAnnotations <- list()
  annotations <- list()
  ageGroupings <- plotData$ageGroup %>% unique()
  cohortIds <- plotData$cohortId %>% unique()


  makeSubPlot <- function(subsetData, colors, title = "", ytitle = "") {
    if (stratifyByCalendarYear) {
      plt <- subsetData %>%
        plotly::plot_ly() %>%
        plotly::add_lines(x = ~calendarYear,
                          color = ~gender,
                          colors = colors,
                          y0 = 0,
                          y = ~incidenceRate) %>%
        plotly::add_markers(x = ~calendarYear,
                            color = ~gender,
                            colors = colors,
                            text = ~tooltip,
                            y0 = 0,
                            opacity = 0.8,
                            y = ~incidenceRate)
    } else {
      plt <- subsetData %>%
        plotly::plot_ly() %>%
        plotly::add_bars(x = ~gender,
                         color = ~gender,
                         colors = colors,
                         text = ~tooltip,
                         y0 = 0,
                         y = ~incidenceRate)
    }

    yaxis <- list(title = list(text = ""),
                  ticklen = 3,
                  ticks = "inside",
                  fixedrange = TRUE)

    if (yscaleFixed) {
      yaxis$range <- c(min(yRange), max(yRange))
    }

    plt <- plt %>%
      plotly::layout(plot_bgcolor = '#eee',
                     xaxis = list(zerolinecolor = '#fff',
                                  zerolinewidth = 0,
                                  showtitle = FALSE,
                                  title = "",
                                  rangemode = "nonnegative",
                                  tickangle = 90,
                                  ticklen = 3,
                                  ticks = "inside",
                                  showgrid = TRUE,
                                  gridcolor = '#fff'),
                     yaxis = yaxis)

    return(plt)
  }

  databaseNames <- plotData$databaseName %>% unique()
  for (dbI in 1:length(databaseNames)) {
    dbm <- databaseNames[dbI]
    subdata <- plotData %>% dplyr::filter(.data$databaseName == dbm)
    for (cj in 1:length(cohortIds)) {
      cohort <- cohortIds[cj]
      csubdata <- subdata %>% dplyr::filter(.data$cohortId == cohort)

      if (stratifyByAgeGroup) {
        for (i in 1:length(ageGroupings)) {
          agrp <- ageGroupings[i]
          cohortName <- ""
          if (i == 1) {
            cohortName <- paste0("C", cohort)
          }
          pltdt <- csubdata %>% dplyr::filter(.data$ageGroup == agrp)

          subplots[[length(subplots) + 1]] <- makeSubPlot(pltdt, colors, ytitle = cohortName)

          xTitlePos <- (length(topAnnotations) / length(ageGroupings)) + (0.50 * 1 / length(ageGroupings))
          if (dbI == 1 && cj == 1) {
            topAnnotations[[length(topAnnotations) + 1]] <- list(text = agrp,
                                                                 x = xTitlePos,
                                                                 y = 1,
                                                                 xref = "paper",
                                                                 yref = "paper",
                                                                 xanchor = "center",
                                                                 yanchor = "bottom",
                                                                 showarrow = FALSE)
          }
        }
      } else {
        subplots[[length(subplots) + 1]] <- makeSubPlot(csubdata, colors, ytitle = paste0("C", cohort))
      }
    }
  }

  j <- 0
  for (i in 1:length(databaseNames)) {
    dbName <- rev(databaseNames)[i]
    ypos <- 1 / (ndatabases * 2) + (1 / ndatabases) * (i - 1)
    topAnnotations[[length(topAnnotations) + 1]] <- list(
      text = dbName,
      x = 1.05,
      showarrow = FALSE,
      y = ypos,
      textangle = 90,
      xref = "paper",
      yref = "paper",
      xanchor = "right",
      yanchor = "middle"
    )

    for (cohort in rev(cohortIds)) {
      cohortYpos <- 1 / (nrows * 2) + (1 / nrows) * j

      topAnnotations[[length(topAnnotations) + 1]] <- list(
        text = paste("C", cohort),
        x = 1.02,
        showarrow = FALSE,
        y = cohortYpos,
        textangle = 90,
        xref = "paper",
        yref = "paper",
        xanchor = "right",
        yanchor = "middle"
      )
      j <- j + 1
    }
  }

  annotations[[length(topAnnotations) + 1]] <- list(
    text = "Incidence Rate (/1000 Person Years)",
    font = list(size = 15),
    x = -0.03,
    showarrow = FALSE,
    y = 0.5,
    textangle = -90,
    xref = "paper",
    yref = "paper",
    xanchor = "center",
    yanchor = "middle"
  )

  plt <- plotly::subplot(subplots, nrows = nrows, shareX = TRUE, shareY = TRUE, margin = c(0.0015, 0.0015, 0.01, 0.01)) %>%
    plotly::layout(annotations = c(annotations, topAnnotations),
                   margin = c(50, 50, 0, 0),
                   showlegend = FALSE,
                   plot_bgcolor = '#eee')

  plt
}

#' incidence Rates View
#' @description
#' Use for customizing UI
#'
#' @param id    Namespace Id - use namespaced id ns("incidenceRates") inside diagnosticsExplorer module
#' @family CohortDiagnostics
#' @export
incidenceRatesView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Incidence Rates",
      width = "100%",
      shiny::htmlTemplate(system.file("cohort-diagnostics-www", "incidenceRate.html", package = utils::packageName()))
    ),
    shinydashboard::box(
      width = NULL,
      status = "primary",

      shiny::fluidRow(
        shiny::column(
          width = 4,
          shiny::checkboxGroupInput(
            inputId = ns("irStratification"),
            label = "Stratify by",
            choices = c("Age", "Sex", "Calendar Year"),
            selected = c("Age", "Sex", "Calendar Year"),
            inline = TRUE
          )
        ),
        shiny::column(
          width = 3,
          shiny::tags$br(),
          shiny::checkboxInput(
            inputId = ns("irYscaleFixed"),
            label = "Use same y-scale across databases"),
        ),
        shiny::column(
          width = 5,
          shiny::conditionalPanel(
            condition = "input.irYscaleFixed",
            ns = ns,
            shiny::sliderInput(
              inputId = ns("YscaleMinAndMax"),
              label = "Limit y-scale range to:",
              min = c(0),
              max = c(0),
              value = c(0, 0),
              dragRange = TRUE, width = 400,
              step = 1,
              sep = "",
            )
          )
        )
      ),
      shiny::fluidRow(
        shiny::conditionalPanel(
          condition = "input.irStratification.indexOf('Age') > -1",
          ns = ns,
          shiny::column(
            width = 6,
            shinyWidgets::pickerInput(
              inputId = ns("incidenceRateAgeFilter"),
              label = "Filter By Age",
              choices = c("All"),
              selected = c("All"),
              multiple = TRUE,
              choicesOpt = list(style = rep_len("color: black;", 999)),
              options = shinyWidgets::pickerOptions(
                actionsBox = TRUE,
                liveSearch = TRUE,
                size = 10,
                dropupAuto = TRUE,
                liveSearchStyle = "contains",
                liveSearchPlaceholder = "Type here to search",
                virtualScroll = 50
              )
            )
          )
        ),
        shiny::conditionalPanel(
          condition = "input.irStratification.indexOf('Sex') > -1",
          ns = ns,
          shiny::column(
            width = 6,
            shinyWidgets::pickerInput(
              inputId = ns("incidenceRateGenderFilter"),
              label = "Filter By Sex",
              choices = c("All"),
              selected = c("All"),
              multiple = TRUE,
              choicesOpt = list(style = rep_len("color: black;", 999)),
              options = shinyWidgets::pickerOptions(
                actionsBox = TRUE,
                liveSearch = TRUE,
                size = 10,
                dropupAuto = TRUE,
                liveSearchStyle = "contains",
                liveSearchPlaceholder = "Type here to search",
                virtualScroll = 50
              )
            )
          )
        )
      ),

      shiny::fluidRow(
        shiny::column(
          width = 2,
          shiny::numericInput(
            inputId = ns("minPersonYear"),
            label = "Minimum person years",
            value = 1000,
            min = 0
          )
        ),
        shiny::column(
          width = 2,
          shiny::numericInput(
            inputId = ns("minSubjectCount"),
            label = "Minimum subject count",
            value = NULL
          )
        ),
        shiny::column(
          width = 2,
          shiny::numericInput(
            inputId = ns("plotRowHeight"),
            label = "Plot row height (pixels)",
            max = 500,
            value = 200,
            min = 100
          )
        ),
        shiny::column(
          width = 5,
          shiny::conditionalPanel(
            condition = "input.irStratification.indexOf('Calendar Year') > -1",
            ns = ns,
            shiny::sliderInput(
              inputId = ns("incidenceRateCalenderFilter"),
              label = "Filter By Calender Year",
              min = c(0),
              max = c(0),
              value = c(0, 0),
              dragRange = TRUE,
              pre = "Year ",
              step = 1,
              sep = ""
            )
          )
        )
      ),
      shiny::actionButton(inputId = ns("generatePlot"), label = "Generate Report"),
    ),

    shiny::conditionalPanel(
      ns = ns,
      condition = "input.generatePlot > 0",

      shiny::uiOutput(outputId = ns("selectedCohorts")),
      shinydashboard::box(
        width = NULL,
        shiny::tabsetPanel(
          id = ns("irPlotTabsetPanel"),
          type = "pills",
          shiny::tabPanel(
            title = "Plot",
            shiny::tags$div(id = ns("plotArea"), height = "100%")
          ),
          shiny::tabPanel(
            title = "Table",
            shiny::fluidRow(
              shiny::column(width = 10, shiny::checkboxInput(ns("groupColumns"), "Group columns by strata/data source", value = FALSE)),
              shiny::column(width = 2, reactableCsvDownloadButton(ns, "irTable"))
            ),
            shinycssloaders::withSpinner(reactable::reactableOutput(ns("irTable"))),
          )
        )
      )
    )
  )
}


incidenceRatesModule <- function(id,
                                 dataSource,
                                 selectedCohorts,
                                 selectedDatabaseIds,
                                 cohortIds,
                                 databaseTable,
                                 cohortTable) {

  shiny::moduleServer(id, function(input, output, session) {
    ns <- session$ns
    irRanges <- getIncidenceRateRanges(dataSource)
    # Incidence rate ---------------------------

    incidenceRateData <- shiny::reactive({
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(length(cohortIds()) > 0, "No cohorts chosen"))
      stratifyByAge <- "Age" %in% input$irStratification
      stratifyByGender <- "Sex" %in% input$irStratification
      stratifyByCalendarYear <-
        "Calendar Year" %in% input$irStratification
      if (length(cohortIds()) > 0) {
        data <- getIncidenceRateResult(
          dataSource = dataSource,
          cohortIds = cohortIds(),
          databaseIds = selectedDatabaseIds(),
          stratifyByGender = stratifyByGender,
          stratifyByAgeGroup = stratifyByAge,
          stratifyByCalendarYear = stratifyByCalendarYear,
          minPersonYears = input$minPersonYear,
          minSubjectCount = input$minSubjectCount
        ) %>%
          dplyr::mutate(incidenceRate = dplyr::case_when(
            .data$incidenceRate < 0 ~ 0,
            TRUE ~ .data$incidenceRate
          ))
      } else {
        data <- NULL
      }
      return(data)
    })

    shiny::observe({
      ageFilter <- irRanges$ageGroups %>%
        dplyr::filter(.data$ageGroup != " ", .data$ageGroup != "NA", !is.na(.data$ageGroup)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(as.integer(sub(
          pattern = "-.+$", "", x = .data$ageGroup
        )))

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateAgeFilter",
        selected = ageFilter$ageGroup,
        choices = ageFilter$ageGroup,
        choicesOpt = list(style = rep_len("color: black;", 999))
      )

    })

    shiny::observe({
      genderFilter <- irRanges$gender %>%
        dplyr::select("gender") %>%
        dplyr::filter(
          .data$gender != "NA",
          .data$gender != " ",
          !is.na(.data$gender),
          !is.null(.data$gender)
        ) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$gender)

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateGenderFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = genderFilter$gender,
        selected = genderFilter$gender
      )

    })

    shiny::observe({
      calenderFilter <- irRanges$calendarYear %>%
        dplyr::select("calendarYear") %>%
        dplyr::filter(
          .data$calendarYear != " ",
          .data$calendarYear != "NA",
          !is.na(.data$calendarYear)
        ) %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear)

      if (nrow(calenderFilter) > 0) {
        minValue <- min(calenderFilter$calendarYear)
        maxValue <- max(calenderFilter$calendarYear)
      } else {
        minValue <- 2010
        maxValue <- 2030
      }

      shiny::updateSliderInput(
        session = session,
        inputId = "incidenceRateCalenderFilter",
        min = minValue,
        max = maxValue,
        value = c(2010, maxValue)
      )
    })

    shiny::observe({
      minIncidenceRateValue <- round(min(irRanges$incidenceRate$minIr), digits = 2)
      maxIncidenceRateValue <- round(max(irRanges$incidenceRate$maxIr), digits = 2)
      shiny::updateSliderInput(
        session = session,
        inputId = "YscaleMinAndMax",
        min = 0,
        max = maxIncidenceRateValue,
        value = c(minIncidenceRateValue, maxIncidenceRateValue),
        step = round((maxIncidenceRateValue - minIncidenceRateValue) / 5, digits = 2)
      )
    })

    incidenceRateCalenderFilter <- shiny::reactive({
      calenderFilter <- incidenceRateData() %>%
        dplyr::select("calendarYear") %>%
        dplyr::filter(
          .data$calendarYear != "NA",
          !is.na(.data$calendarYear)
        ) %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear)

      calenderFilter <-
        calenderFilter[calenderFilter$calendarYear >= input$incidenceRateCalenderFilter[1] &
                         calenderFilter$calendarYear <= input$incidenceRateCalenderFilter[2], , drop = FALSE] %>%
          dplyr::pull("calendarYear")
      return(calenderFilter)
    })


    incidenceRateYScaleFilter <- shiny::reactive({
      input$YscaleMinAndMax
    })

    nplots <- shiny::reactive({
      nPlotsMade <- length(selectedDatabaseIds()) * length(cohortIds())
      if ("Age" %in% input$irStratification) {
        nPlotsMade <- nPlotsMade * length(incidenceRateCalenderFilter())
      }

      return(nPlotsMade)
    })

    shiny::observeEvent(input$generatePlot, {
      rowHeight <- ifelse(is.null(input$plotRowHeight) | is.na(input$plotRowHeight), 200, input$plotRowHeight)
      plotHeight <- rowHeight *
        length(selectedDatabaseIds()) *
        length(cohortIds())
      shiny::removeUI(selector = paste0("#", ns("irPlotContainer")))
      shiny::insertUI(
        selector = paste0("#", ns("plotArea")),
        ui = shiny::div(
          id = ns("irPlotContainer"),
          shinycssloaders::withSpinner(
            plotly::plotlyOutput(
              outputId = ns("incidenceRatePlot"),
              width = "100%",
              height = sprintf("%spx", plotHeight)
            )
          ),
          height = sprintf("%spx", plotHeight + 50)
        )
      )
    })


    incidenceRateDataFiltered <- shiny::reactive({
      data <- incidenceRateData()

      stratifyByAge <- "Age" %in% input$irStratification
      stratifyByGender <- "Sex" %in% input$irStratification
      stratifyByCalendarYear <-
        "Calendar Year" %in% input$irStratification

      if (stratifyByAge && !"All" %in% input$incidenceRateAgeFilter) {
        data <- data %>%
          dplyr::filter(.data$ageGroup %in% input$incidenceRateAgeFilter)
      }
      if (stratifyByGender &&
        !"All" %in% input$incidenceRateGenderFilter) {
        data <- data %>%
          dplyr::filter(.data$gender %in% input$incidenceRateGenderFilter)
      }
      if (stratifyByCalendarYear) {
        data <- data %>%
          dplyr::filter(.data$calendarYear %in% incidenceRateCalenderFilter())
      }

      return(data)
    })

    getIrPlot <- shiny::eventReactive(input$generatePlot, {
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(length(cohortIds()) > 0, "No cohorts chosen"))
      nPlotsMade <- nplots()

      shiny::withProgress(
        message = paste(
          "Building incidence rate plot data for ",
          length(cohortIds()),
          " cohorts and ",
          length(selectedDatabaseIds()),
          " databases"
        ),
      {
        data <- incidenceRateDataFiltered()

        shiny::validate(shiny::need(all(!is.null(data), nrow(data) > 0), paste0("No data for this combination")))

        stratifyByAge <- "Age" %in% input$irStratification
        stratifyByGender <- "Sex" %in% input$irStratification
        stratifyByCalendarYear <-
          "Calendar Year" %in% input$irStratification

        if (all(!is.null(data), nrow(data) > 0)) {
          plot <- plotIncidenceRate(
            data = data,
            cohortTable = cohortTable,
            stratifyByAgeGroup = stratifyByAge,
            stratifyByGender = stratifyByGender,
            stratifyByCalendarYear = stratifyByCalendarYear,
            yscaleFixed = input$irYscaleFixed,
            yRange = incidenceRateYScaleFilter()
          )
          return(plot)
        }
      },
        detail = "Please Wait"
      )

    })

    output$incidenceRatePlot <- plotly::renderPlotly(expr = {
      getIrPlot()
    })


    irTableData <- shiny::eventReactive(input$generatePlot, {
      data <- incidenceRateDataFiltered()

      data <- data %>%
        dplyr::inner_join(cohortTable, by = "cohortId") %>%
        dplyr::mutate(incidenceProportion = .data$cohortCount / .data$cohortSubjects) %>%
        dplyr::select("cohortName",
                      "databaseName",
                      "ageGroup",
                      "calendarYear",
                      "gender",
                      "personYears",
                      "cohortCount",
                      "incidenceRate",
                      "incidenceProportion")

      barChart <- function(label, width = "100%", height = "1rem", fill = "#337ab7", background = "#ccc") {
        bar <- shiny::div(style = list(background = fill, width = width, height = height))
        chart <- shiny::div(style = list(flexGrow = 1, marginLeft = "0.5rem", background = background), bar)
        shiny::div(style = list(display = "flex", alignItems = "center"), label, chart)
      }

      columnDefs <- list(
        "cohortName" = reactable::colDef(name = "Cohort", minWidth = 250),
        "databaseName" = reactable::colDef(name = "Database", minWidth = 250),
        "cohortCount" = reactable::colDef(header = withTooltip("Events",
                                                               "Number of subjects in cohort within strata")),
        "personYears" = reactable::colDef(header = withTooltip("Person Years",
                                                               "Cumulative time (in years)"),
                                          cell = function(value) {
                                            scales::comma(value, accuracy = 0.01)
                                          },
                                          format = reactable::colFormat(digits = 2)),
        "incidenceRate" = reactable::colDef(header = withTooltip("Inicidence per 1k/py",
                                                                 "Incidence of event per 1000 person years - (Events/Person Years * 1000)"),
                                            cell = function(value) {
                                              width <- paste0(value / max(data$incidenceRate) * 100, "%")
                                              barChart(sprintf("%.2f", value), width = width)
                                            },
                                            format = reactable::colFormat(digits = 3)),
        "incidenceProportion" = reactable::colDef(header = withTooltip("Inicidence proportion",
                                                                       "Proportion of cohort - Event count in strata / total cohort count"),
                                                  cell = function(value) {
                                                    value <- abs(value)
                                                    width <- paste0(value * 100, "%")
                                                    barChart(sprintf("%.2f%%", value * 100), width = width)
                                                  },
                                                  format = reactable::colFormat(digits = 3))

      )

      groupBy <- c("cohortName", "databaseName")
      sorted <- c("cohortName")

      if (!"Age" %in% input$irStratification) {
        data <- data %>% dplyr::select(-"ageGroup")
      } else {
        groupBy <- c(groupBy, "ageGroup")
        sorted <- c(sorted, "ageGroup")
        columnDefs$ageGroup <- reactable::colDef(name = "Age Group")
      }

      if (!"Sex" %in% input$irStratification) {
        data <- data %>% dplyr::select(-"gender")
      } else {
        groupBy <- c(groupBy, "gender")
      }

      if (!"Calendar Year" %in% input$irStratification) {
        data <- data %>% dplyr::select(-"calendarYear")
      } else {
        sorted <- c(sorted, "calendarYear")
        columnDefs$calendarYear <- reactable::colDef(name = "Year")
      }

      # modifiable args list to call reactable::reactable
      return(list(data = data,
                  groupBy = groupBy,
                  defaultSorted = sorted,
                  columns = columnDefs,
                  searchable = TRUE,
                  striped = TRUE))
    })

    output$irTable <- reactable::renderReactable({
      args <- irTableData()
      # Masking groupBy as NULL causes bug in reactable=
      if (isFALSE(input$groupColumns)) {
        args <- within(args, rm(groupBy))
      }
      do.call(reactable::reactable, args)
    })

    selectionsOutput <- shiny::eventReactive(input$generatePlot, {
      databases <- databaseTable %>% dplyr::filter(.data$databaseId %in% selectedDatabaseIds())

      shinydashboard::box(
        status = "warning",
        width = "100%",
        shiny::fluidRow(
          shiny::column(
            width = 7,
            shiny::tags$b("Selected cohorts :"),
            shiny::tagList(lapply(selectedCohorts(), shiny::tags$p))
          ),
          shiny::column(
            width = 5,
            shiny::tags$b("Selected databases :"),
            shiny::tagList(lapply(databases$databaseName, shiny::tags$p))
          )
        )
      )
    })

  output$selectedCohorts <- shiny::renderUI({ selectionsOutput() })

  })
}
