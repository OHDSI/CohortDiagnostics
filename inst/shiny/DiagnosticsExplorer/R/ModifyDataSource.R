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
      dplyr::distinct(.data$cohortId, .data$cohortName)
    data <- data %>%
      dplyr::inner_join(shortNameRef, by = "cohortId")
    return(data)
  }


addMetaDataInformationToResults <- function(data) {
  if (is.null(data) || nrow(data) == 0) {
    return(dplyr::tibble(Note = paste0(
      "No data available for selected databases and cohorts"
    )))
  }
  if (nrow(data) > 0) {
    data <- data %>%
      dplyr::left_join(
        y = cohort %>%
          dplyr::select(.data$cohortId,
                        .data$phenotypeId,
                        .data$cohortName),
        by = c('cohortId')
      ) %>%
      dplyr::left_join(
        y = phenotypeDescription %>%
          dplyr::select(.data$phenotypeId, .data$phenotypeName),
        by = "phenotypeId"
      ) %>%
      dplyr::relocate(
        .data$databaseId,
        .data$phenotypeId,
        .data$phenotypeName,
        .data$cohortId,
        .data$cohortName
      ) %>%
      dplyr::arrange(.data$phenotypeName, .data$cohortName)
  }
  return(data)
}


# Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
isEmpty <-
  function(connection,
           resultsDatabaseSchema,
           tableName) {
    sql <-
      sprintf("SELECT 1 FROM %s.%s LIMIT 1;",
              resultsDatabaseSchema,
              tableName)
    oneRow <-
      DatabaseConnector::dbGetQuery(conn = connection, sql) %>%
      dplyr::tibble()
    return(nrow(oneRow) == 0)
  }