getCountsForHeaderForUseInDataTable <-
  function(dataSource,
           cohortIds,
           databaseIds,
           source = "Datasource Level",
           fields = "Both") {
    
    if (all(!hasData(databaseIds),
            !hasData(cohortIds))) {
      stop("Please provide either databaseIds or cohortids")
    }
    
    if (source == "Datasource Level") {
      countsForHeader <- getDatabaseCounts(dataSource = dataSource,
                                                  databaseIds = databaseIds)
      if (!hasData(countsForHeader)) {
        warning("Did not get counts for table header in metadata file. Please check the output from Cohort Diagnostics (metadata file is generated in the last step, is it in the zip file?), is it corrupted?")
      }
    } else if (source == "Cohort Level") {
     countsForHeader <-
        getCohortCountResult(
          dataSource = dataSource,
          cohortIds = cohortIds,
          databaseIds = databaseIds
        ) %>%
        dplyr::rename(records = .data$cohortEntries,
                      persons = .data$cohortSubjects)
      # dplyr::select(-.data$cohortId) #only one cohort id is supported
      if (!hasData(countsForHeader)) {
        warning("Did not get counts for table header in cohort table. Please check the output from Cohort Diagnostics, is it corrupted?")
      }
    }
    
    if (fields  %in% c("Persons")) {
      countsForHeader <- countsForHeader %>%
        dplyr::select(-.data$records) %>%
        dplyr::rename(count = .data$persons)
    } else if (fields %in% c("Events","Records")) {
      countsForHeader <- countsForHeader %>%
        dplyr::select(-.data$persons) %>%
        dplyr::rename(count = .data$records)
    }
    
    return(countsForHeader)
}


loadResultsTable <- function(tableName, required = FALSE) {
  if (required || tableName %in% resultsTablesOnServer) {
    tryCatch({
      table <- DatabaseConnector::dbReadTable(connectionPool,
                                              paste(resultsDatabaseSchema, tableName, sep = "."))
    }, error = function(err) {
      stop(
        "Error reading from ",
        paste(resultsDatabaseSchema, tableName, sep = "."),
        ": ",
        err$message
      )
    })
    colnames(table) <-
      SqlRender::snakeCaseToCamelCase(colnames(table))
    if (nrow(table) > 0) {
      assign(
        SqlRender::snakeCaseToCamelCase(tableName),
        dplyr::as_tibble(table),
        envir = .GlobalEnv
      )
    }
  }
}


# Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
isEmpty <- function(tableName) {
  sql <-
    sprintf("SELECT 1 FROM %s.%s LIMIT 1;",
            resultsDatabaseSchema,
            tableName)
  oneRow <- DatabaseConnector::dbGetQuery(connectionPool, sql)
  return(nrow(oneRow) == 0)
}



# borrowed from https://stackoverflow.com/questions/19747384/create-new-column-in-dataframe-based-on-partial-string-matching-other-column
patternReplacement <- function(x, patterns, replacements = patterns, fill = NA, ...)
{
  stopifnot(length(patterns) == length(replacements))
  
  ans = rep_len(as.character(fill), length(x))    
  empty = seq_along(x)
  
  for (i in seq_along(patterns)) {
    greps = grepl(patterns[[i]], x[empty], ...)
    ans[empty[greps]] = replacements[[i]]  
    empty = empty[!greps]
  }
  return(ans)
}

getTimeAsInteger <- function(time = Sys.time(),
                             tz = "UTC") {
  return(as.numeric(as.POSIXlt(time, tz = tz)))
}

getTimeFromInteger <- function(x, tz = "UTC") {
  originDate <- as.POSIXct("1970-01-01", tz = tz)
  originDate <- originDate + x
  return(originDate)
}

capitalizeFirstLetter <- function(x) {
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  x
}

getReactTableWithColumnsGroupedByDatabaseId <- function(data,
                                                        cohort = NULL,
                                                        database = NULL,
                                                        headerCount = NULL,
                                                        keyColumns,
                                                        dataColumns,
                                                        countLocation,
                                                        maxCount,
                                                        sort = TRUE,
                                                        showResultsAsPercent = FALSE,
                                                        rowSpan = 2,
                                                        showAllRows = FALSE,
                                                        valueFill = 0) {
  if (is.null(cohort)) {
    warning("cohort table is missing")
    cohort <- data %>%
      dplyr::select(.data$cohortId) %>%
      dplyr::distinct() %>%
      dplyr::mutate(shortName = paste0("C", .data$cohortId))
  }
  if (!'shortName' %in% colnames(cohort)) {
    cohort <- cohort %>%
      dplyr::mutate(shortName = paste0("C", .data$cohortId))
  }
  
  if (is.null(database)) {
    warning("database table is missing")
    database <- data %>%
      dplyr::select(.data$databaseId) %>%
      dplyr::distinct() %>%
      dplyr::mutate(id = dplyr::row_number()) %>%
      dplyr::mutate(shortName = paste0("D", .data$id))
  }
  if (!'shortName' %in% colnames(database)) {
    database <- database %>%
      dplyr::distinct() %>%
      dplyr::mutate(id = dplyr::row_number()) %>%
      dplyr::mutate(shortName = paste0("D", .data$id))
  }
  
  # ensure the data has required fields
  keyColumns <- keyColumns %>% unique()
  dataColumns <- dataColumns %>% unique()
  missingColumns <-
    setdiff(x = c(keyColumns, dataColumns) %>% unique(),
            y = colnames(data))
  if (length(missingColumns) > 0)  {
    stop(
      paste0(
        "Improper specification for sketch, following fields are missing in data ",
        paste0(missingColumns, collapse = ", ")
      )
    )
  }
  
  if (showResultsAsPercent) {
    for (i in (1:length(dataColumns))) {
      if ("sequence" %in% colnames(data)) {
        for (j in 1:ceiling(max(data$sequence) / 100))
          data[data$sequence > (j - 1) * 100 &
                 data$sequence < j * 100,][[dataColumns[i]]] <-
            round(data[data$sequence > (j - 1) * 100 &
                         data$sequence < j * 100,][[dataColumns[i]]] /
                    sum(data[data$sequence > (j - 1) * 100 &
                               data$sequence < j * 100,][[dataColumns[i]]], na.rm = TRUE), 2)
      } else {
        data[[dataColumns[i]]] <- round(data[[dataColumns[i]]] / sum(data[[dataColumns[i]]], na.rm = TRUE), 2)
      }
    }
  }
  
  #error during wrapup: Can't convert <double> to <list> in pivot_wider
  distinctDatabaseId <- data$databaseId %>%  unique()
  data <- data %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(dataColumns),
      names_to = "type",
      values_to = "valuesData"
    ) %>%
    dplyr::mutate(type = paste0(
      .data$databaseId,
      "-",
      .data$type
    )) %>% 
    tidyr::pivot_wider(
      id_cols = dplyr::all_of(keyColumns),
      names_from = "type",
      values_from = "valuesData",
      values_fill = valueFill
    )
  
  if (sort) {
    sortByColumns <- colnames(data)
    sortByColumns <-
      sortByColumns[stringr::str_detect(string = sortByColumns,
                                        pattern = paste(dataColumns, collapse = "|"))]
    if (length(sortByColumns) > 0) {
      sortByColumns <- sortByColumns[[1]]
      data <- data %>%
        dplyr::arrange(dplyr::desc(dplyr::across(dplyr::all_of(sortByColumns))))
    }
  }
  
  dataColumns <-
    colnames(data)[stringr::str_detect(
      string = colnames(data),
      pattern = paste0(keyColumns, collapse = "|"),
      negate = TRUE
    )]
  
  columnDefinitions <- list()
  for (i in (1:length(keyColumns))) {
    columnName <- camelCaseToTitleCase(colnames(data)[i])
    colnames(data)[which(names(data) == keyColumns[i])]  <- columnName
    columnDefinitions[[columnName]] <-
      reactable::colDef(
        name = columnName,
        sortable = TRUE,
        resizable = TRUE,
        filterable = TRUE,
        show = TRUE,
        minWidth = 200,
        html = TRUE,
        na = "",
        align = "left"
      )
  }
  
  maxValue <- 0
  if (valueFill == 0) {
    for (i in (1:length(dataColumns))) {
      maxValue <- max(maxValue,max(data[dataColumns[i]], na.rm = TRUE))
    }
  }
  
  for (i in (1:length(dataColumns))) {
    columnNameWithDatabaseAndCount <-   stringr::str_split(dataColumns[i],"-")[[1]]
    columnName <- columnNameWithDatabaseAndCount[2]
  
    if (!is.null(headerCount)){
      if (countLocation == 2) {
        filteredHeaderCount <- headerCount %>% 
          dplyr::filter(.data$databaseId ==  columnNameWithDatabaseAndCount[1])
      
        columnCount <- filteredHeaderCount[[columnName]]
        columnName <- paste0(columnName," (", scales::comma(columnCount), ")")
      }
    }
    
    columnDefinitions[[dataColumns[i]]] <-
      reactable::colDef(
        name =  camelCaseToTitleCase(columnName),
        cell =  minCellDefReactable(showResultsAsPercent),
        sortable = TRUE,
        resizable = TRUE,
        filterable = TRUE,
        show = TRUE,
        minWidth = 200,
        html = TRUE,
        na = "",
        align = "left",
        style = function(value) {
          if (class(value) != "character") {
            list(
              backgroundImage = sprintf("linear-gradient(90deg, %1$s %2$s, transparent %2$s)", "#9ccee7", paste0((value / maxValue) * 100, "%")),
              backgroundSize = paste("100%", "100%"),
              backgroundRepeat = "no-repeat",
              backgroundPosition = "center",
              color = "#000"
            )
          } else {
            list()
          }
        }
      )
    
    
  }
  
  columnGroups <- list()
  for (i in 1:length(distinctDatabaseId)) {
    extractedDataColumns <- dataColumns[stringr::str_detect(
      string = dataColumns, 
      pattern = distinctDatabaseId[i] 
    )]
    
    
    columnName <- distinctDatabaseId[i]
    
    if (!is.null(headerCount)) {
      if (countLocation == 1) {
        columnName <- headerCount %>% 
          dplyr::filter(.data$databaseId ==  distinctDatabaseId[i]) %>% 
          dplyr::mutate(count = paste0(.data$databaseId," (",scales::comma(.data$count),")")) %>% 
          dplyr::pull(.data$count)
      }
    }
    
    columnGroups[[i]] <- 
      reactable::colGroup(name = columnName, 
                          columns = extractedDataColumns)
  }
  
  dataTable <- 
    reactable::reactable(data = data,
                         columns = columnDefinitions,
                         columnGroups = columnGroups,
                         sortable = TRUE,
                         resizable = TRUE,
                         filterable = TRUE,
                         searchable = TRUE,
                         pagination = TRUE,
                         showPagination = TRUE,
                         showPageInfo = TRUE,
                         # # minRows = 100, # to change based on number of rows in data
                         highlight = TRUE,
                         striped = TRUE,
                         compact = TRUE,
                         wrap = FALSE,
                         showSortIcon = TRUE,
                         showSortable = TRUE,
                         fullWidth = TRUE,
                         bordered = TRUE,
                         showPageSizeOptions = TRUE,
                         pageSizeOptions = c(10, 20, 50, 100, 1000),
                         defaultPageSize = ifelse(showAllRows, nrow(data),20) ,
                         selection = 'single',
                         onClick = "select",
                         theme = reactable::reactableTheme(
                           rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
                         )
    ) 
  
  return(dataTable)
}

getSimpleReactable <- function(data,
                               keyColumns,
                               dataColumns,
                               selection = NULL,
                               showResultsAsPercent = FALSE,
                               defaultSelected = NULL) {
  columnDefinitions <- list()
  
  for (i in (1:length(keyColumns))) {
    columnName <- camelCaseToTitleCase(keyColumns[i])
    colnames(data)[which(names(data) == keyColumns[i])]  <- columnName
    columnDefinitions[[columnName]] <-
      reactable::colDef(
        name = columnName,
        minWidth = 250,
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
    for (i in (1:length(dataColumns))) {
      maxValue <- max(data[dataColumns[i]], na.rm = TRUE)
    }
    
    for (i in (1:length(dataColumns))) {
      columnName <- camelCaseToTitleCase(dataColumns[i])
      colnames(data)[which(names(data) == dataColumns[i])]  <- columnName
      columnDefinitions[[columnName]] <-
        reactable::colDef(
          name = columnName,
          cell = minCellDefReactable(showResultsAsPercent),
          sortable = TRUE,
          resizable = TRUE,
          filterable = TRUE,
          show = TRUE,
          html = TRUE,
          na = "",
          align = "left",
          style = function(value) {
            list(
              backgroundImage = sprintf("linear-gradient(90deg, %1$s %2$s, transparent %2$s)", "#9ccee7", paste0((value / maxValue) * 100, "%")),
              backgroundSize = paste("100%", "100%"),
              backgroundRepeat = "no-repeat",
              backgroundPosition = "center",
              color = "#000"
            )
          }
        )
    }
  }
  
  dataTable <- reactable::reactable(data = data,
                                    columns = columnDefinitions,
                                    sortable = TRUE,
                                    resizable = TRUE, 
                                    filterable = TRUE,
                                    searchable = TRUE, 
                                    pagination = TRUE, 
                                    showPagination = TRUE, 
                                    showPageInfo = TRUE,
                                    # minRows = 100, # to change based on number of rows in data
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
                                    defaultPageSize = 20,
                                    theme = reactable::reactableTheme(
                                      rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
                                    )
  )
  
  return(dataTable)
}

getMaxValueForStringMatchedColumnsInDataFrame <- function(data, string) {
  data %>% 
    dplyr::summarise(dplyr::across(dplyr::contains(string), ~ max(.x, na.rm = TRUE))) %>% 
    tidyr::pivot_longer(values_to = "value", cols = dplyr::everything()) %>% 
    dplyr::pull() %>% 
    max(na.rm = TRUE)
}