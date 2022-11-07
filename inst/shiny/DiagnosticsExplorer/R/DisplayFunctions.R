formatDataCellValueInDisplayTable <-
  function(showDataAsPercent = FALSE) {
    if (showDataAsPercent) {
      reactable::JS(
        "function(data) {
          if (isNaN(parseFloat(data.value))) return data.value;
          if (Number.isInteger(data.value) && data.value > 0) return (100 * data.value).toFixed(0).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
          if (data.value > 999) return (100 * data.value).toFixed(2).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
          if (data.value < 0) return '<' + (Math.abs(data.value) * 100).toFixed(2) + '%';
          return (100 * data.value).toFixed(1) + '%';
        }"
      )
    } else {
      reactable::JS(
        "function(data) {
          if (isNaN(parseFloat(data.value))) return data.value;
          if (Number.isInteger(data.value) && data.value > 0) return data.value.toFixed(0).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
          if (data.value > 999) return data.value.toFixed(1).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
          if (data.value < 0) return  '<' + Math.abs(data.value.toFixed(3));
          return data.value.toFixed(1);
        }"
      )
    }
  }

copyToClipboardButton <-
  function(toCopyId,
           label = "Copy to clipboard",
           icon = shiny::icon("clipboard"),
           ...) {
    script <- sprintf(
      "
  text = document.getElementById('%s').textContent;
  html = document.getElementById('%s').innerHTML;
  function listener(e) {
    e.clipboardData.setData('text/html', html);
    e.clipboardData.setData('text/plain', text);
    e.preventDefault();
  }
  document.addEventListener('copy', listener);
  document.execCommand('copy');
  document.removeEventListener('copy', listener);
  return false;",
      toCopyId,
      toCopyId
    )

    tags$button(
      type = "button",
      class = "btn btn-default action-button",
      onclick = script,
      icon,
      label,
      ...
    )
  }


getDisplayTableHeaderCount <-
  function(dataSource,
           cohortIds,
           databaseIds,
           source = "Datasource",
           fields = "Both") {
    if (source == "Datasource") {
      countsForHeader <- getDatabaseCounts(
        dataSource = dataSource,
        databaseIds = databaseIds
      )
    } else if (source == "cohort") {
      countsForHeader <-
        getResultsCohortCounts(
          dataSource = dataSource,
          cohortIds = cohortIds,
          databaseIds = databaseIds
        ) %>%
          dplyr::rename(
            records = cohortEntries,
            persons = cohortSubjects
          )
    }

    if (fields %in% c("Persons")) {
      countsForHeader <- countsForHeader %>%
        dplyr::select(-records) %>%
        dplyr::rename(count = persons)
    } else if (fields %in% c("Events", "Records")) {
      countsForHeader <- countsForHeader %>%
        dplyr::select(-persons) %>%
        dplyr::rename(count = records)
    }
    return(countsForHeader)
  }


prepDataForDisplay <- function(data,
                               keyColumns,
                               dataColumns) {
  # ensure the data has required fields
  keyColumns <- c(keyColumns %>% unique())
  dataColumns <- dataColumns %>% unique()
  commonColumns <- intersect(
    colnames(data),
    c(keyColumns, dataColumns, "databaseId", "temporalChoices")
  ) %>% unique()

  missingColumns <-
    setdiff(
      x = c(keyColumns, dataColumns) %>% unique(),
      y = colnames(data)
    )
  if (length(missingColumns) > 0 && missingColumns != "") {
    stop(
      paste0(
        "Improper specification for sketch, following fields are missing in data ",
        paste0(missingColumns, collapse = ", ")
      )
    )
  }
  data <- data %>%
    dplyr::select(dplyr::all_of(commonColumns))

  if ("databaseId" %in% colnames(data)) {
    data <- data %>%
      dplyr::relocate("databaseId")
  }
  return(data)
}

pallete <- function(x) {
  cr <- colorRamp(c("white", "#9ccee7"))
  col <- "#ffffff"
  tryCatch({
    if (x > 1.0) {
      x <- 1
    }

    col <- rgb(cr(x), maxColorValue = 255)
  }, error = function(...) {
  })
  return(col)
}

getDisplayTableGroupedByDatabaseId <- function(data,
                                               cohort,
                                               databaseTable,
                                               headerCount = NULL,
                                               keyColumns,
                                               dataColumns,
                                               countLocation,
                                               maxCount,
                                               sort = TRUE,
                                               showDataAsPercent = FALSE,
                                               excludedColumnFromPercentage = NULL,
                                               pageSize = 20,
                                               valueFill = 0,
                                               selection = NULL,
                                               isTemporal = FALSE) {
  data <- prepDataForDisplay(
    data = data,
    keyColumns = keyColumns,
    dataColumns = dataColumns
  )

  data <- data %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(dataColumns),
      names_to = "type",
      values_to = "valuesData"
    )

  data <- data %>%
    dplyr::inner_join(databaseTable %>%
                        dplyr::select(databaseId, databaseName),
                      by = "databaseId")

  if (isTemporal) {
    data <- data %>%
      dplyr::mutate(type = paste0(
        databaseId,
        "-",
        temporalChoices,
        "_sep_",
        type
      ))
    distinctColumnGroups <- data$temporalChoices %>% unique()
  } else {
    data <- data %>%
      dplyr::mutate(type = paste0(
        databaseId,
        "_sep_",
        type
      ))
    distinctColumnGroups <- data$databaseId %>% unique()
  }

  data <- data %>%
    tidyr::pivot_wider(
      id_cols = dplyr::all_of(keyColumns),
      names_from = "type",
      values_from = "valuesData"
    )

  if (sort) {
    sortByColumns <- colnames(data)
    sortByColumns <-
      sortByColumns[stringr::str_detect(
        string = sortByColumns,
        pattern = paste(dataColumns, collapse = "|")
      )]
    if (length(sortByColumns) > 0) {
      sortByColumns <- sortByColumns[[1]]
      data <- data %>%
        dplyr::arrange(dplyr::desc(dplyr::across(dplyr::all_of(
          sortByColumns
        ))))
    }
  }

  dataColumns <-
    colnames(data)[stringr::str_detect(
      string = colnames(data),
      pattern = paste0(keyColumns, collapse = "|"),
      negate = TRUE
    )]

  columnDefinitions <- list()
  columnTotalMinWidth <- 0
  columnTotalMaxWidth <- 0

  for (i in (1:length(keyColumns))) {
    columnName <- SqlRender::camelCaseToTitleCase(colnames(data)[i])
    displayTableColumnMinMaxWidth <-
      getDisplayTableColumnMinMaxWidth(
        data = data,
        columnName = keyColumns[[i]]
      )
    columnTotalMinWidth <-
      columnTotalMinWidth + displayTableColumnMinMaxWidth$minValue
    columnTotalMaxWidth <-
      columnTotalMaxWidth + displayTableColumnMinMaxWidth$maxValue
    if (class(data[[keyColumns[[i]]]]) == "logical") {
      data[[keyColumns[[i]]]] <- ifelse(data[[keyColumns[[i]]]],
                                        as.character(icon("check")), ""
      )
    }

    colnames(data)[which(names(data) == keyColumns[i])] <-
      columnName
    columnDefinitions[[columnName]] <-
      reactable::colDef(
        name = columnName,
        sortable = sort,
        resizable = TRUE,
        filterable = TRUE,
        show = TRUE,
        minWidth = displayTableColumnMinMaxWidth$minValue,
        maxWidth = displayTableColumnMinMaxWidth$maxValue,
        html = TRUE,
        na = "",
        align = "left"
      )
  }

  maxValue <- 0
  if (valueFill == 0) {
    maxValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = dataColumns)
  }

  for (i in (1:length(dataColumns))) {
    columnNameWithDatabaseAndCount <-
      stringr::str_split(dataColumns[i], "_sep_")[[1]]
    columnName <- columnNameWithDatabaseAndCount[2]
    displayTableColumnMinMaxWidth <-
      getDisplayTableColumnMinMaxWidth(
        data = data,
        columnName = columnName
      )
    columnTotalMinWidth <- columnTotalMinWidth + 200
    columnTotalMaxWidth <- columnTotalMaxWidth + 200

    if (!is.null(headerCount)) {
      if (countLocation == 2) {
        filteredHeaderCount <- headerCount %>%
          dplyr::filter(databaseId == columnNameWithDatabaseAndCount[1])
        columnCount <- filteredHeaderCount[[columnName]]
        columnName <-
          paste0(columnName, " (", scales::comma(columnCount), ")")
      }
    }
    showPercent <- showDataAsPercent
    if (showDataAsPercent &&
      !is.null(excludedColumnFromPercentage)) {
      if (stringr::str_detect(
        tolower(dataColumns[i]),
        tolower(excludedColumnFromPercentage)
      )) {
        showPercent <- FALSE
      }
    }
    columnDefinitions[[dataColumns[i]]] <-
      reactable::colDef(
        name = SqlRender::camelCaseToTitleCase(columnName),
        cell = formatDataCellValueInDisplayTable(showDataAsPercent = showPercent),
        sortable = sort,
        resizable = FALSE,
        filterable = TRUE,
        show = TRUE,
        minWidth = 200,
        maxWidth = 200,
        html = TRUE,
        na = "",
        align = "left",
        style = function(value) {
          color <- '#fff'
          dt <- data[[dataColumns[i]]]
          if (is.list(dt)) {
            dt <- dt %>% unlist()
          }
          if (is.numeric(value) & hasData(dt)) {
            value <- ifelse(is.na(value), min(dt, na.rm = TRUE), value)
            normalized <- (value - min(dt, na.rm = TRUE)) / (max(dt, na.rm = TRUE) - min(dt, na.rm = TRUE))
            color <- pallete(normalized)
          }
          list(background = color)
        }
      )
  }
  if (columnTotalMaxWidth > 1300) {
    columnTotalMaxWidth <- "auto"
    columnTotalMinWidth <- "auto"
  }

  dbNameMap <- list()
  for (i in 1:nrow(databaseTable)) {
    dbNameMap[[databaseTable[i,]$databaseId]] <- databaseTable[i,]$databaseName
  }


  columnGroups <- list()
  for (i in 1:length(distinctColumnGroups)) {
    extractedDataColumns <-
      dataColumns[stringr::str_detect(
        string = dataColumns,
        pattern = stringr::fixed(distinctColumnGroups[i])
      )]

    columnName <- dbNameMap[[distinctColumnGroups[i]]]

    if (!is.null(headerCount)) {
      if (countLocation == 1) {
        columnName <- headerCount %>%
          dplyr::filter(databaseId == distinctColumnGroups[i]) %>%
          dplyr::mutate(count = paste0(
            databaseName,
            " (",
            scales::comma(count),
            ")"
          )) %>%
          dplyr::pull(count)
      }
    }
    columnGroups[[i]] <-
      reactable::colGroup(
        name = columnName,
        columns = extractedDataColumns
      )
  }

  dataTable <-
    reactable::reactable(
      data = data,
      columns = columnDefinitions,
      columnGroups = columnGroups,
      sortable = sort,
      resizable = TRUE,
      filterable = TRUE,
      searchable = TRUE,
      pagination = TRUE,
      showPagination = TRUE,
      showPageInfo = TRUE,
      highlight = TRUE,
      striped = TRUE,
      compact = TRUE,
      wrap = FALSE,
      showSortIcon = sort,
      showSortable = sort,
      fullWidth = TRUE,
      bordered = TRUE,
      showPageSizeOptions = TRUE,
      pageSizeOptions = c(10, 20, 50, 100, 1000),
      defaultPageSize = pageSize,
      selection = selection,
      onClick = "select",
      style = list(maxWidth = columnTotalMaxWidth, minWidth = columnTotalMinWidth),
      theme = reactable::reactableTheme(
        rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
      )
    )
  return(dataTable)
}


getDisplayTableSimple <- function(data,
                                  keyColumns,
                                  dataColumns,
                                  selection = NULL,
                                  showDataAsPercent = FALSE,
                                  defaultSelected = NULL,
                                  databaseTable = NULL,
                                  pageSize = 20) {
  data <- prepDataForDisplay(
    data = data,
    keyColumns = keyColumns,
    dataColumns = dataColumns
  )

  columnDefinitions <- list()
  for (i in (1:length(keyColumns))) {
    columnName <- SqlRender::camelCaseToTitleCase(keyColumns[i])

    displayTableColumnMinMaxWidth <-
      getDisplayTableColumnMinMaxWidth(
        data = data,
        columnName = keyColumns[[i]]
      )

    colnames(data)[which(names(data) == keyColumns[i])] <-
      columnName

    columnDefinitions[[columnName]] <-
      reactable::colDef(
        name = columnName,
        cell = if ("logical" %in% class(data[[columnName]])) {
          function(value) {
            if (value) {
              "\u2714\ufe0f"
            } else {
              "\u274C"
            }
          }
        },
        minWidth = displayTableColumnMinMaxWidth$minValue,
        maxWidth = displayTableColumnMinMaxWidth$maxValue,
        sortable = TRUE,
        resizable = TRUE,
        filterable = TRUE,
        show = TRUE,
        html = TRUE,
        na = "",
        align = "left"
      )
  }

  if (hasData(dataColumns)) {
    maxValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = dataColumns)

    for (i in (1:length(dataColumns))) {
      columnName <- SqlRender::camelCaseToTitleCase(dataColumns[i])
      colnames(data)[which(names(data) == dataColumns[i])] <- columnName
      columnDefinitions[[columnName]] <- reactable::colDef(
        name = columnName,
        cell = formatDataCellValueInDisplayTable(showDataAsPercent = showDataAsPercent),
        sortable = TRUE,
        resizable = FALSE,
        filterable = TRUE,
        show = TRUE,
        html = TRUE,
        na = "",
        align = "left",
        style = function(value) {
          color <- '#fff'
          if (is.numeric(value) & hasData(data[[columnName]])) {
            value <- ifelse(is.na(value), min(data[[columnName]], na.rm = TRUE), value)
            normalized <- (value - min(data[[columnName]], na.rm = TRUE)) / (maxValue - min(data[[columnName]], na.rm = TRUE))
            color <- pallete(normalized)
          }
          list(background = color)
        }
      )
    }
  }

  dataTable <- reactable::reactable(
    data = data,
    columns = columnDefinitions,
    sortable = TRUE,
    resizable = TRUE,
    filterable = TRUE,
    searchable = TRUE,
    pagination = TRUE,
    showPagination = TRUE,
    showPageInfo = TRUE,
    highlight = TRUE,
    striped = TRUE,
    compact = TRUE,
    wrap = FALSE,
    showSortIcon = TRUE,
    showSortable = TRUE,
    fullWidth = TRUE,
    bordered = TRUE,
    selection = selection,
    defaultSelected = defaultSelected,
    onClick = "select",
    showPageSizeOptions = TRUE,
    pageSizeOptions = c(10, 20, 50, 100, 1000),
    defaultPageSize = pageSize,
    theme = reactable::reactableTheme(
      rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
    )
  )
  return(dataTable)
}

# This is bad
getMaxValueForStringMatchedColumnsInDataFrame <-
  function(data, string) {
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
      dplyr::filter(!is.na(value)) %>%
      dplyr::pull(value)
    
    if (is.list(data)) {
      data <- data %>% unlist()
    }
    
    if (!hasData(data)) {
      return(0)
    } else {
      return(max(data, na.rm = TRUE))
    }
  }


getDisplayTableColumnMinMaxWidth <- function(data,
                                             columnName,
                                             pixelMultipler = 10,
                                             # approximate number of pixels per character
                                             padPixel = 25,
                                             maxWidth = NULL,
                                             minWidth = 10 * pixelMultipler) {
  columnNameFormatted <- SqlRender::camelCaseToTitleCase(columnName)

  if ("character" %in% class(data[[columnName]])) {
    maxWidth <- (max(stringr::str_length(
      c(
        stringr::str_replace_na(
          string = data[[columnName]],
          replacement = ""
        ),
        columnNameFormatted
      )
    )) * pixelMultipler) + padPixel # to pad for table icon like sort
    minWidth <-
      min(
        stringr::str_length(columnNameFormatted) * pixelMultipler,
        maxWidth
      ) + padPixel
  }

  if ("logical" %in% class(data[[columnName]])) {
    maxWidth <-
      max(stringr::str_length(columnNameFormatted) * pixelMultipler,
          na.rm = TRUE
      ) + padPixel
    minWidth <-
      (stringr::str_length(columnNameFormatted) * pixelMultipler) + padPixel
  }

  if ("numeric" %in% class(data[[columnName]])) {
    maxWidth <-
      (max(stringr::str_length(
        c(
          as.character(data[[columnName]]),
          columnNameFormatted
        )
      ), na.rm = TRUE) * pixelMultipler) + padPixel # to pad for table icon like sort
    minWidth <-
      min(stringr::str_length(columnNameFormatted) * pixelMultipler,
          maxWidth,
          na.rm = TRUE
      ) + padPixel
  }

  data <- list(
    minValue = minWidth,
    maxValue = maxWidth
  )
  return(data)
}


csvDownloadButton <- function(ns,
                              outputTableId,
                              buttonText = "Download CSV (filtered)") {

  shiny::tagList(
    shiny::tags$br(),
    shiny::tags$button(buttonText,
                       onclick = paste0("Reactable.downloadDataCSV('", ns(outputTableId), "')")))
}
