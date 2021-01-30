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

addMetaDataInformationToResults <- function(data) {
  data <- data %>%
    dplyr::left_join(y = cohort %>% 
                       dplyr::select(.data$cohortId, 
                                     .data$phenotypeName,
                                     .data$cohortName),
                     by = c('cohortId')) %>%
    dplyr::relocate(
      .data$databaseId,
      .data$phenotypeName,
      .data$cohortId,
      .data$cohortName
    ) %>%
    dplyr::mutate(
      databaseId = as.factor(.data$databaseId),
      phenotypeName = as.factor(.data$phenotypeName),
      cohortId = as.factor(.data$cohortId),
      cohortName = as.factor(.data$cohortName)) %>% 
    dplyr::arrange(.data$phenotypeName, .data$cohortName)
  
  if (is.null(data) || nrow(data) == 0) {
    return(dplyr::tibble(
      Note = paste0("No data available for selected databases and cohorts")
    ))
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