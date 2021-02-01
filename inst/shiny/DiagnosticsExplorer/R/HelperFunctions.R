cohortReference <- function(outputId) {
  shinydashboard::box(# title = "Reference",
    status = "warning",
    width = "100%",
    shiny::uiOutput(outputId = outputId))
}

standardDataTable <- function(data, 
                              selectionMode = "single",
                              searching = TRUE) {
  
  dataTableFilter =
    list(position = 'top',
         clear = TRUE,
         plain = FALSE)
  
  dataTableOption =
    list(
      pageLength = 10,
      lengthMenu = c(5, 10, 15, 20, 100, 500, 1000),
      lengthChange = TRUE,
      searching = searching,
      ordering = TRUE,
      scrollX = TRUE,
      ordering = TRUE,
      paging = TRUE,
      info = TRUE,
      searchHighlight = TRUE,
      # search = list(regex = TRUE, caseInsensitive = FALSE),
      stateSave = TRUE,
      dom = 'lBfrtip',
      # B for buttons
      buttons = list(
        'copy',
        list(
          extend = 'collection',
          buttons = c('csv', 'excel', 'pdf'),
          text = 'Download'
        ),
        'print',
        'colvis'
      ),
      colReorder = TRUE,
      realtime = FALSE,
      # for col reorder
      # fixedColumns = list(leftColumns = 1),
      # fixedHeader = TRUE,
      # processing = TRUE,
      autoWidth = TRUE
    )
  
  dataTable <- DT::datatable(
    data = data,
    class = "stripe compact order-column hover",
    rownames = FALSE,
    options = dataTableOption,
    colnames = colnames(data) %>% camelCaseToTitleCase(),
    filter = dataTableFilter,
    # style = 'bootstrap4',
    escape = FALSE,
    selection = list(mode = selectionMode, target = "row"),
    editable = FALSE,
    # container = sketch,
    extensions = c('Buttons', 'ColReorder', 'FixedColumns', 'FixedHeader'),
    plugins = c('natural') #'ellipsis'
    # escape = FALSE
  )
  listOfVariablesThatAreAlwaysFactors <- c('domainId',
                                           'conceptClassId',
                                           'vocabularyId',
                                           'standardConcept',
                                           'conceptSetName',
                                           'conceptName',
                                           'cohortId',
                                           'cohortName',
                                           'phenotypeId',
                                           'phenotypeName',
                                           'analysisName',
                                           'startDay',
                                           'endDay',
                                           'analysisId'
                                           )
  
  convertVariableToFactor <- function(data, variables) {
    for (i in (1:length(variables))) {
      variable <- variables[i]
      if (variable %in% colnames(data)) {    
        data[[variable]] <- as.factor(data[[variable]]) %>%
          dplyr::tibble()
      }
    }
    return(data)
  }
  
  colNames <- colnames(data)
  listRounds <-
    colNames[stringr::str_detect(string = tolower(colNames),
                                 pattern = 'entries|subjects|count|min|max|p10|p25|median|p75|p90|max|before|onvisitstart|after|duringvisit')]
  listDecimal <-
    colNames[stringr::str_detect(string = tolower(colNames),
                                 pattern = 'average|standarddeviation|mean|sd|personyears|incidencerate')]
  listPercent <-
    colNames[stringr::str_detect(string = tolower(colNames),
                                 pattern = 'percent')]
  if (length(listRounds) > 0) {
    dataTable <- DT::formatRound(table = dataTable,
                                 columns = listRounds,
                                 digits = 0)
  }
  if (length(listDecimal) > 0) {
    dataTable <- DT::formatRound(table = dataTable,
                                 columns = listDecimal,
                                 digits = 2)
  }
  if (length(listPercent) > 0) {
    dataTable <- DT::formatPercentage(table = dataTable,
                                      columns = listPercent,
                                      digits = 1)
  }
  return(dataTable)
}

# Infoboxes ------------------------------------------------------------------------
addInfo <- function(item, infoId) {
  infoTag <- tags$small(
    class = "badge pull-right action-button",
    style = "padding: 1px 6px 2px 6px; background-color: steelblue;",
    type = "button",
    id = infoId,
    "i"
  )
  item$children[[1]]$children <-
    append(item$children[[1]]$children, list(infoTag))
  return(item)
}

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