shinyServer(function(input, output, session) {
  
  subject <- reactiveValues(index = 1)
  
  queryResult <- reactive({
    if (is.null(input$domains)) {
      return(data.frame())
    } else {
      events <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = eventSql,
        cdm_database_schema = cdmDatabaseSchema,
        subject_id = subjectIds[subject$index],
        drug_era = "Drug era" %in% input$domains,
        drug_exposure = "Drug exposure" %in% input$domains,
        condition_era = "Condition era" %in% input$domains,
        condition_occurrence = "Condition occurrence" %in% input$domains,
        procedure = "Procedure" %in% input$domains,
        measurement = "Measurement" %in% input$domains,
        observation = "Observation" %in% input$domains,
        visit = "Visit" %in% input$domains,
        observation_period = "Observation period" %in% input$domains,
        vocabulary_database_schema = vocabularyDatabaseSchema, 
        snakeCaseToCamelCase = TRUE, 
        tempEmulationSchema = tempEmulationSchema
      ) %>% 
        dplyr::tibble()
      colnames(events)[colnames(events) == "domainName"] <- "domain"
      return(events)
    }
  })
  
  filteredEvents <- reactive({
    events <- queryResult()
    if (nrow(events) != 0 && input$filterRegex != "") {
      events <-
        events[grepl(input$filterRegex, events$conceptName, ignore.case = TRUE) |
                 events$domain == "Visit" |
                 events$domain == "Observation period",]
    }
    if (nrow(events) != 0) {
      events <- events[order(events$conceptId),]
      getY <- function(subset) {
        uniqueConceptIds <- unique(subset$conceptId)
        subset$y <- match(subset$conceptId, uniqueConceptIds)
        return(subset)
      }
      events <- lapply(split(events, events$domain), getY)
      events <- do.call("rbind", events)
    }
    return(events)
  })
  
  colorScale <- reactive({
    domains <- c("Cohort", input$domains)
    if (length(domains) == 1) {
      colors <- c("Red")
    } else {
      temp <-
        RColorBrewer::brewer.pal(max(3, length(domains) - 1), "Set2")
      colors <- c("Red", temp[1:(length(domains) - 1)])
    }
    names(colors) <- domains
    return(colors)
  })
  
  observeEvent(input$nextButton, {
    if (subject$index < length(subjectIds)) {
      subject$index <- subject$index + 1
    }
  })
  
  observeEvent(input$previousButton, {
    if (subject$index > 1) {
      subject$index <- subject$index - 1
    }
  })
  
  output$subjectId <- renderText({
    return(subjectIds[subject$index])
  })
  
  output$age <- renderText({
    return(cohort$age[cohort$subjectId ==  subjectIds[subject$index]][1])
  })
  
  output$gender <- renderText({
    return(cohort$gender[cohort$subjectId ==  subjectIds[subject$index]][1])
  })
  
  camelCaseToPretty <- function(string) {
    string <- gsub("([A-Z])", " \\1", string)
    substr(string, 1, 1) <- toupper(substr(string, 1, 1))
    return(string)
  }
  
  output$eventTable <- renderDataTable({
    events <- filteredEvents()
    events$y <- NULL
    cohortsOfSubject <-
      which(cohort$subjectId == subjectIds[subject$index])
    for (i in cohortsOfSubject) {
      events <- rbind(
        events,
        data.frame(
          domain = "Cohort",
          conceptId = cohortDefinitionId,
          conceptName = "Cohort entry",
          typeConceptName = "",
          startDate = cohort$cohortStartDate[i],
          endDate = cohort$cohortEndDate[i]
        )
      )
    }
    events <- events[order(events$startDate),]
    
    truncScript <- "function(data, type, row, meta) {\n
      return type === 'display' && data != null && data.length > %s ?\n
        '<span title=\"' + data + '\">' + data.substr(0, %s) + '...</span>' : data;\n
     }"
    options = list(
      pageLength = 10000,
      searching = FALSE,
      lengthChange = FALSE,
      ordering = FALSE,
      paging = FALSE,
      scrollY = '75vh',
      columnDefs = list(list(
        targets = 2,
        render = JS(sprintf(truncScript, 90, 90))
      ),
      list(
        targets = 3,
        render = JS(sprintf(truncScript, 40, 40))
      ))
    )
    
    selection = list(mode = "single", target = "row")
    table <- datatable(
      events,
      options = options,
      selection = selection,
      rownames = FALSE,
      colnames = camelCaseToPretty(colnames(events)),
      class = "stripe nowrap compact"
    )
    colors <- colorScale()
    table <- formatStyle(
      table = table,
      columns = 1,
      target = "cell",
      backgroundColor = styleEqual(names(colors), colors)
    )
    return(table)
  })
  
  output$plotSmall <- renderPlotly(plot())
  output$plotBig <- renderPlotly(plot())
  
  plot <- reactive({
    events <- filteredEvents()
    if (nrow(events) == 0) {
      return(NULL)
    } else {
      colors <- colorScale()
      domains <- aggregate(y ~ domain, events, max)
      domains <- domains[order(domains$domain),]
      domains$offset <- cumsum(domains$y) - domains$y
      events <- merge(events, domains[, c("domain", "offset")])
      events$y <- events$y + events$offset
      yRange <- c(min(events$y) - 1, max(events$y) + 1)
      events$text <-
        sprintf(
          "%s - %s<br>%s<br>%s",
          events$startDate,
          events$endDate,
          events$conceptName,
          events$typeConceptName
        )
      eventsPerY <- aggregate(domain ~ y, data = events, length)
      yGrid <- eventsPerY$y[eventsPerY$domain > 1]
      
      yAxis <- list(
        title = "",
        tickmode = "array",
        tickvals = yGrid,
        zeroline = FALSE,
        showline = FALSE,
        showticklabels = FALSE,
        showgrid = TRUE,
        range = yRange,
        fixedrange = TRUE
      )
      xAxis <- list(
        title = "",
        zeroline = FALSE,
        showline = FALSE,
        showticklabels = TRUE,
        showgrid = TRUE
      )
      plot <- plot_ly() %>%
        add_trace(
          data = events,
          x = ~ startDate,
          y = ~ y,
          color = ~ domain,
          colors = colors,
          type = 'scatter',
          mode = 'markers',
          text = ~ text,
          hovertemplate = "%{text}"
        ) %>%
        add_segments(
          data = events,
          x = ~ startDate,
          y = ~ y,
          xend = ~ endDate,
          yend = ~ y,
          color = ~ domain,
          showlegend = FALSE,
          hoverinfo = "skip"
        )
      
      shapes <- list()
      cohortsOfSubject <-
        which(cohort$subjectId == subjectIds[subject$index])
      first <- TRUE
      for (i in cohortsOfSubject) {
        data <- data.frame(
          date = rep(cohort$cohortStartDate[i], 2),
          y = rep(yRange, 2),
          text = sprintf(
            "%s - %s",
            cohort$cohortStartDate[i],
            cohort$cohortEndDate[i]
          )
        )
        plot <- plot %>% add_lines(
          x = ~ date,
          y = ~ y,
          data = data,
          mode = "lines",
          line = list(color = colors["Cohort"]),
          name = "Cohort",
          text = ~ text,
          hovertemplate = "%{text}",
          showlegend = first
        )
        first <- FALSE
        if (!is.na(cohort$cohortEndDate[i])) {
          shapes[[length(shapes) + 1]] <- list(
            type = "rect",
            fillcolor = "red",
            line = list(color = colors["Cohort"]),
            opacity = 0.3,
            x0 = cohort$cohortStartDate[i],
            x1 = cohort$cohortEndDate[i],
            xref = "startDate",
            y0 = yRange[1],
            y1 = yRange[2],
            yref = "y"
          )
        }
      }
      plot <- plot %>% layout(
        yaxis = yAxis,
        xaxis = xAxis,
        shapes = shapes,
        legend = list(orientation = 'h'),
        margin =  list(
          l = 1,
          r = 1,
          b = 1,
          t = 25,
          pad = 1
        )
      )
      plot
    }
  })
  
  observeEvent(input$filterInfo, {
    showModal(
      modalDialog(
        title = "Concept Name Filter",
        easyClose = TRUE,
        footer = NULL,
        size = "l",
        HTML(
          "Filter the concept to include in the plot and table by concept name using a regular expression.
           For example, the regular expression 'celecox|diclof' finds concepts like 'Celecoxib 200mg Oral Tablet' and 'Diclofenac'.
           See <a href='https://en.wikipedia.org/wiki/Regular_expression'>Wikipedia</a> for more information on regular expressions."
        )
      )
    )
  })
})
