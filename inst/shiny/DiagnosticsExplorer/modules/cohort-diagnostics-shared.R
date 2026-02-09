# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
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

hasData <- function(data) {
  if (is.null(data)) {
    return(FALSE)
  }
  if (is.data.frame(data)) {
    if (nrow(data) == 0) {
      return(FALSE)
    }
  }
  if (!is.data.frame(data)) {
    if (length(data) == 0) {
      return(FALSE)
    }
    if (length(data) == 1) {
      if (is.na(data)) {
        return(FALSE)
      }
    }
  }
  return(TRUE)
}

# Useful if you want to include isBinary in an invisible column and then have the result display as a percentage or
# Not depending on this value
formatCellByBinaryType <- function() {
  reactable::JS(
    "function(data) {
      let binaryCol = data.allCells.find(x => x.column.id == 'isBinary')
      if (binaryCol !== undefined) {
        if(binaryCol.value == 'Y') {
          if (isNaN(parseFloat(data.value))) return data.value;
          if (Number.isInteger(data.value) && data.value > 0) return (100 * data.value).toFixed(0).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
          if (data.value > 999) return (100 * data.value).toFixed(2).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
          if (data.value < 0) return '<' + (Math.abs(data.value) * 100).toFixed(2) + '%';
          return  (100 * data.value).toFixed(1) + '%';
        }
      }
      if (isNaN(parseFloat(data.value))) return data.value;
      if (Number.isInteger(data.value) && data.value > 0) return data.value.toFixed(0).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
      if (data.value > 999) return data.value.toFixed(1).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
      if (data.value < 0) return  '<' + Math.abs(data.value.toFixed(3));

      return data.value.toFixed(1);
    }")
}


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


getResultsCohortCounts <- function(dataSource,
                                   cohortIds = NULL,
                                   databaseIds = NULL) {
  sql <- "SELECT cc.*
            FROM  @schema.@table_name cc
            WHERE cc.cohort_id IS NOT NULL
            {@use_database_ids} ? { AND cc.database_id in (@database_ids)}
            {@cohort_ids != ''} ? {  AND cc.cohort_id in (@cohort_ids)}
            ;"
  
  data <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      cohort_ids = cohortIds,
      use_database_ids = !is.null(databaseIds),
      database_ids = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_count")
    ) %>%
      tidyr::tibble()
  
  data <- merge(data, dataSource$dbTable, by = 'databaseId')
  

  return(data)
}

getDatabaseCounts <- function(dataSource,
                              databaseIds) {
  sql <- "SELECT *
              FROM  @schema.@database_table
              WHERE database_id in (@database_ids);"
  data <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      database_ids = quoteLiterals(databaseIds),
      database_table = dataSource$databaseTable,
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  return(data)
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
            "records" = "cohortEntries",
            "persons" = "cohortSubjects"
          )
    }

    if (fields %in% c("Persons")) {
      countsForHeader <- countsForHeader %>%
        dplyr::select(-"records") %>%
        dplyr::rename("count" = "persons")
    } else if (fields %in% c("Events", "Records")) {
      countsForHeader <- countsForHeader %>%
        dplyr::select(-"persons") %>%
        dplyr::rename("count" = "records")
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
  cr <- grDevices::colorRamp(c("white", "#9ccee7"))
  col <- "#ffffff"
  tryCatch({
    if (x > 1.0) {
      x <- 1
    }

    col <- grDevices::rgb(cr(x), maxColorValue = 255)
  }, error = function(...) {
  })
  return(col)
}

# NOTE - this table is very messy and hard to change.
# We should move to a model where aspects of this are reused but tables are always modifiable in a simple manner
# Changing this function
getDisplayTableGroupedByDatabaseId <- function(data,
                                               databaseTable,
                                               headerCount = NULL,
                                               keyColumns,
                                               dataColumns,
                                               countLocation,
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
                        dplyr::select("databaseId", "databaseName"),
                      by = "databaseId")

  if (isTemporal) {
    data <- data %>%
      dplyr::mutate(type = paste0(
        .data$databaseId,
        "-",
        .data$temporalChoices,
        "_sep_",
        .data$type
      ))
    distinctColumnGroups <- data$temporalChoices %>% unique()
  } else {
    data <- data %>%
      dplyr::mutate(type = paste0(
        .data$databaseId,
        "_sep_",
        .data$type
      ))
    distinctColumnGroups <- data$databaseId %>% unique()
  }

  data <- data %>%
    dplyr::distinct() %>%
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
    if (inherits(data[[keyColumns[[i]]]], "logical")) {
      data[[keyColumns[[i]]]] <- ifelse(data[[keyColumns[[i]]]],
                                        as.character(shiny::icon("check")), ""
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
      getMaxValByString(data = data, string = dataColumns)
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
          dplyr::filter(.data$databaseId == columnNameWithDatabaseAndCount[1])
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
            normalized <- (value - min(dt, na.rm = TRUE)) / (CohortDiagnostics::safeMax(dt) - min(dt, na.rm = TRUE))
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
          dplyr::filter(.data$databaseId == distinctColumnGroups[i]) %>%
          dplyr::mutate(count = paste0(
            .data$databaseName,
            " (",
            scales::comma(.data$count),
            ")"
          )) %>%
          dplyr::pull("count")
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
      getMaxValByString(data = data, string = dataColumns)

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
getMaxValByString <-
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
      dplyr::filter(!is.na(.data$value)) %>%
      dplyr::pull("value")

    if (is.list(data)) {
      data <- data %>% unlist()
    }

    if (!hasData(data)) {
      return(0)
    } else {
      return(CohortDiagnostics::safeMax(data))
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
    maxWidth <- (CohortDiagnostics::safeMax(stringr::str_length(
      c(
        stringr::str_replace_na(
          string = data[[columnName]],
          replacement = ""
        ),
        columnNameFormatted
      )
    )) * pixelMultipler) + padPixel # to pad for table icon like sort
    minWidth <-
      min( # replace stringr::str_length with nchar()
        stringr::str_length(columnNameFormatted) * pixelMultipler,
        maxWidth
      ) + padPixel
  }

  if ("logical" %in% class(data[[columnName]])) {
    maxWidth <-
      CohortDiagnostics::safeMax(stringr::str_length(columnNameFormatted) * pixelMultipler) + padPixel
    minWidth <-
      (stringr::str_length(columnNameFormatted) * pixelMultipler) + padPixel
  }

  if ("numeric" %in% class(data[[columnName]])) {
    maxWidth <-
      (CohortDiagnostics::safeMax(stringr::str_length(
        c(
          as.character(data[[columnName]]),
          columnNameFormatted
        )
      )) * pixelMultipler) + padPixel # to pad for table icon like sort
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

resolvedConceptSet <- function(dataSource,
                               databaseIds,
                               cohortId,
                               conceptSetId = NULL) {
  sqlResolved <- "SELECT DISTINCT rc.cohort_id,
                    	rc.concept_set_id,
                    	c.concept_id,
                    	c.concept_name,
                    	c.domain_id,
                    	c.vocabulary_id,
                    	c.concept_class_id,
                    	c.standard_concept,
                    	c.concept_code,
                    	rc.database_id
                    FROM @schema.@resolved_concepts_table rc
                    LEFT JOIN @schema.@concept_table c
                    ON rc.concept_id = c.concept_id
                    WHERE rc.database_id IN (@database_ids)
                    	AND rc.cohort_id = @cohortId
                      {@concept_set_id != \"\"} ? { AND rc.concept_set_id IN (@concept_set_id)}
                    ORDER BY c.concept_id;"
  resolved <-
    dataSource$connectionHandler$queryDb(
      sql = sqlResolved,
      schema = dataSource$schema,
      database_ids = quoteLiterals(databaseIds),
      cohortId = cohortId,
      concept_set_id = conceptSetId,
      resolved_concepts_table = dataSource$prefixTable("resolved_concepts"),
      concept_table = dataSource$prefixTable("concept"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble() %>%
      dplyr::arrange(.data$conceptId)

  return(resolved)
}

mappedConceptSet <- function(dataSource,
                             databaseIds,
                             cohortId) {
  sqlMapped <-
    "WITH resolved_concepts_mapped
    AS (
    	SELECT concept_sets.concept_id AS resolved_concept_id,
    		c1.concept_id,
    		c1.concept_name,
    		c1.domain_id,
    		c1.vocabulary_id,
    		c1.concept_class_id,
    		c1.standard_concept,
    		c1.concept_code
    	FROM (
    		SELECT DISTINCT concept_id
    		FROM @schema.@resolved_concepts
    		WHERE database_id IN (@databaseIds)
    			AND cohort_id = @cohort_id
    		) concept_sets
    	INNER JOIN @schema.@concept_relationship cr ON concept_sets.concept_id = cr.concept_id_2
    	INNER JOIN @schema.@concept c1 ON cr.concept_id_1 = c1.concept_id
    	WHERE relationship_id = 'Maps to'
    		AND standard_concept IS NULL
    	)
    SELECT
        c.database_id,
    	c.cohort_id,
    	c.concept_set_id,
    	mapped.*
    FROM (SELECT DISTINCT concept_id, database_id, cohort_id, concept_set_id FROM @schema.@resolved_concepts) c
    INNER JOIN resolved_concepts_mapped mapped ON c.concept_id = mapped.resolved_concept_id
    {@cohort_id != ''} ? { WHERE c.cohort_id = @cohort_id};
    "
  mapped <-
    dataSource$connectionHandler$queryDb(
      sql = sqlMapped,
      schema = dataSource$schema,
      databaseIds = quoteLiterals(databaseIds),
      concept = dataSource$prefixTable("concept"),
      concept_relationship = dataSource$prefixTable("concept_relationship"),
      resolved_concepts = dataSource$prefixTable("resolved_concepts"),
      cohort_id = cohortId,
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble() %>%
      dplyr::arrange(.data$resolvedConceptId)
  return(mapped)
}

addShortName <-
  function(data,
           shortNameRef = NULL,
           cohortIdColumn = "cohortId",
           shortNameColumn = "shortName") {
    if (is.null(shortNameRef)) {
      shortNameRef <- data %>%
        dplyr::distinct(.data$cohortId) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::mutate(shortName = paste0("C", dplyr::row_number()))
    }

    shortNameRef <- shortNameRef %>%
      dplyr::distinct(.data$cohortId, .data$shortName)
    colnames(shortNameRef) <- c(cohortIdColumn, shortNameColumn)
    data <- data %>%
      dplyr::inner_join(shortNameRef, by = cohortIdColumn)
    return(data)
  }


checkErrorCohortIdsDatabaseIds <- function(errorMessage,
                                           cohortIds,
                                           databaseIds) {
  checkmate::assertNumeric(
    x = cohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2^53,
    any.missing = FALSE,
    add = errorMessage
  )
  checkmate::assertCharacter(
    x = databaseIds,
    min.len = 1,
    any.missing = FALSE,
    unique = TRUE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  return(errorMessage)
}

quoteLiterals <- function(x) {
  if (is.null(x)) {
    return("")
  } else {
    return(paste0("'", paste(x, collapse = "', '"), "'"))
  }
}

queryResultCovariateValue <- function(dataSource,
                                      cohortIds,
                                      analysisIds = NULL,
                                      databaseIds,
                                      startDay = NULL,
                                      endDay = NULL,
                                      temporalCovariateValue = TRUE,
                                      temporalCovariateValueDist = TRUE,
                                      meanThreshold = 0) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = startDay,
    any.missing = TRUE,
    unique = FALSE,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::assertIntegerish(
    x = endDay,
    any.missing = TRUE,
    unique = FALSE,
    null.ok = TRUE,
    add = errorMessage
  )

  temporalTimeRefData <-
    dataSource$connectionHandler$queryDb(
      sql = "SELECT *
             FROM @schema.@table_name
             WHERE (time_id IS NOT NULL AND time_id != 0)
              {@start_day != \"\"} ? { AND start_day IN (@start_day)}
              {@end_day != \"\"} ? { AND end_day IN (@end_day)};",
      snakeCaseToCamelCase = TRUE,
      schema = dataSource$schema,
      table_name = dataSource$prefixTable("temporal_time_ref"),
      start_day = startDay,
      end_day = endDay
    ) %>%
      dplyr::tibble()

  temporalTimeRefData <- dplyr::bind_rows(
    temporalTimeRefData,
    dplyr::tibble(timeId = -1)
  )

  temporalAnalysisRefData <-
    dataSource$connectionHandler$queryDb(
      sql = "SELECT *
             FROM @schema.@table_name
              WHERE analysis_id IS NOT NULL
                {@analysis_ids != \"\"} ? { AND analysis_id IN (@analysis_ids)}
              ;",
      analysis_ids = analysisIds,
      table_name = dataSource$prefixTable("temporal_analysis_ref"),
      snakeCaseToCamelCase = TRUE,
      schema = dataSource$schema
    ) %>%
      dplyr::tibble()

  temporalCovariateRefData <-
    dataSource$connectionHandler$queryDb(
      sql = "SELECT *
             FROM @schema.@table_name
              WHERE covariate_id IS NOT NULL
                {@analysis_ids != \"\"} ? { AND analysis_id IN (@analysis_ids)};",
      snakeCaseToCamelCase = TRUE,
      analysis_ids = analysisIds,
      table_name = dataSource$prefixTable("temporal_covariate_ref"),
      schema = dataSource$schema
    ) %>%
      dplyr::tibble()

  temporalCovariateValueData <- NULL
  if (temporalCovariateValue) {
    temporalCovariateValueData <-
      dataSource$connectionHandler$queryDb(
        sql = "SELECT tcv.*
                FROM @schema.@table_name tcv
                INNER JOIN @schema.@ref_table_name ref ON ref.covariate_id = tcv.covariate_id
                WHERE ref.covariate_id IS NOT NULL
                {@analysis_ids != \"\"} ? { AND ref.analysis_id IN (@analysis_ids)}
                {@cohort_id != \"\"} ? { AND tcv.cohort_id IN (@cohort_id)}
                {@time_id != \"\"} ? { AND (time_id IN (@time_id) OR time_id IS NULL OR time_id = 0)}
                {@use_database_id} ? { AND database_id IN (@database_id)}
                {@filter_mean_threshold != \"\"} ? { AND tcv.mean > @filter_mean_threshold};",
        snakeCaseToCamelCase = TRUE,
        analysis_ids = analysisIds,
        time_id = temporalTimeRefData$timeId %>% unique(),
        use_database_id = !is.null(databaseIds),
        database_id = quoteLiterals(databaseIds),
        table_name = dataSource$prefixTable("temporal_covariate_value"),
        ref_table_name = dataSource$prefixTable("temporal_covariate_ref"),
        cohort_id = cohortIds,
        schema = dataSource$schema,
        filter_mean_threshold = meanThreshold
      ) %>%
        dplyr::tibble() %>%
        tidyr::replace_na(replace = list(timeId = -1))
  }

  temporalCovariateValueDistData <- NULL
  if (temporalCovariateValueDist) {
    temporalCovariateValueDistData <-
      dataSource$connectionHandler$queryDb(
        sql = "SELECT *
             FROM @schema.@table_name tcv
              WHERE covariate_id IS NOT NULL
                {@covariate_id != \"\"} ? { AND covariate_id IN (@covariate_id)}
                {@cohort_id != \"\"} ? { AND cohort_id IN (@cohort_id)}
                {@time_id != \"\"} ? { AND (time_id IN (@time_id) OR time_id IS NULL OR time_id = 0)}
                {@use_database_id} ? { AND database_id IN (@database_id)}
                {@filter_mean_threshold != \"\"} ? { AND tcv.mean > @filter_mean_threshold};",
        snakeCaseToCamelCase = TRUE,
        covariate_id = temporalCovariateRefData$covariateId %>% unique(),
        time_id = temporalTimeRefData$timeId %>% unique(),
        use_database_id = !is.null(databaseIds),
        database_id = quoteLiterals(databaseIds),
        cohort_id = cohortIds,
        table_name = dataSource$prefixTable("temporal_covariate_value_dist"),
        schema = dataSource$schema,
        filter_mean_threshold = meanThreshold
      ) %>%
        dplyr::tibble() %>%
        tidyr::replace_na(replace = list(timeId = -1))
  }

  if (hasData(temporalCovariateValueData)) {
    temporalCovariateValueData <- temporalCovariateValueData %>%
      dplyr::left_join(temporalTimeRefData,
                       by = "timeId"
      )
  }

  if (hasData(temporalCovariateValueDistData)) {
    temporalCovariateValueDistData <-
      temporalCovariateValueDistData %>%
        dplyr::left_join(temporalTimeRefData,
                         by = "timeId"
        )
  }

  data <- list(
    temporalTimeRef = temporalTimeRefData,
    temporalAnalysisRef = temporalAnalysisRefData,
    temporalCovariateRef = temporalCovariateRefData,
    temporalCovariateValue = temporalCovariateValueData,
    temporalCovariateValueDist = temporalCovariateValueDistData
  )
  return(data)
}

# modeId = 0 -- Events
# modeId = 1 -- Persons
getInclusionRuleStats <- function(dataSource,
                                  cohortIds = NULL,
                                  databaseIds,
                                  modeId = 1) {
  sql <- "SELECT *
    FROM  @schema.@table_name
    WHERE database_id in (@database_id)
    {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
    ;"

  inclusion <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_inclusion"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  inclusionResults <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_inc_result"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  inclusionStats <-
    dataSource$connectionHandler$queryDb(
      sql = sql,
      schema = dataSource$schema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_inc_stats"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()


  if (!hasData(inclusion) || !hasData(inclusionStats)) {
    return(NULL)
  }

  result <- inclusion %>%
    dplyr::select("cohortId", "databaseId", "ruleSequence", "name") %>%
    dplyr::distinct() %>%
    dplyr::left_join(
      inclusionStats %>%
        dplyr::filter(.data$modeId == !!modeId) %>%
        dplyr::select(
          "cohortId",
          "databaseId",
          "ruleSequence",
          "personCount",
          "gainCount",
          "personTotal"
        ),
      by = c("cohortId", "databaseId", "ruleSequence")
    ) %>%
    dplyr::arrange(.data$cohortId,
                   .data$databaseId,
                   .data$ruleSequence) %>%
    dplyr::mutate(remain = 0)

  inclusionResults <- inclusionResults %>%
    dplyr::filter(.data$modeId == !!modeId)

  combis <- result %>%
    dplyr::select("cohortId",
                  "databaseId") %>%
    dplyr::distinct()

  resultFinal <- c()
  for (j in (1:nrow(combis))) {
    combi <- combis[j,]
    data <- result %>%
      dplyr::inner_join(combi,
                        by = c("cohortId", "databaseId"))

    inclusionResult <- inclusionResults %>%
      dplyr::inner_join(combi,
                        by = c("cohortId", "databaseId"))
    mask <- 0
    for (ruleId in (0:(nrow(data) - 1))) {
      mask <- bitwOr(mask, 2^ruleId) #bitwise OR operation: if both are 0, then 0; else 1
      idx <-
        bitwAnd(inclusionResult$inclusionRuleMask, mask) == mask
      data$remain[data$ruleSequence == ruleId] <-
        sum(inclusionResult$personCount[idx])
    }
    resultFinal[[j]] <- data
  }
  resultFinal <- dplyr::bind_rows(resultFinal) %>%
    dplyr::rename(
      "meetSubjects" = "personCount",
      "gainSubjects" = "gainCount",
      "remainSubjects" = "remain",
      "totalSubjects" = "personTotal",
      "ruleName" = "name",
      "ruleSequenceId" = "ruleSequence"
    ) %>%
    dplyr::select(
      "cohortId",
      "ruleSequenceId",
      "ruleName",
      "meetSubjects",
      "gainSubjects",
      "remainSubjects",
      "totalSubjects",
      "databaseId"
    )
  return(resultFinal)
}
