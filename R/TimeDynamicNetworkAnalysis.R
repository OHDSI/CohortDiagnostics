#' Time Dynamic Network Analysis
#' @description
#' ALl events that occur in a patients history can be viewed as states in a time dynamic network model.
#' In this approach we make a series of simplifying assumtions to compute a directed, weighted, time dynamic graph.
#'
#' Events fall in time windows relative to index date, as specified by the timeBinSize parameter.
#'
#' For example a condition occurrence 7 days prior to or post index
#'
#' Nodes represent covariates, edges represent the coccurence at different time points
#'
#' This network is not exported for individuals, but only in the aggregate with the objective of finding common pathways
#' prior to index and post index.
#' @export
#' @param timeBinSize        The time window to determine that events co-occur
#' @param timeWindow         vector of length 2, the time in days before and after index to include in analysis
#' @param cdSettings         CohortDiagnostics settings object
#' @param ...                Global  cdSettings
runTimeDynamicNetworkAnalysis <- function (...,
                                           timeBinSize = 7,
                                           timeWindow = c(-365, 365),
                                           cdSettings = NULL) {
  checkmate::assertIntegerish(timeBinSize, len = 1)
  checkmate::assertIntegerish(timeWindow, len = 2)

  if (is.null(cdSettings)) {
    cdSettings <- createCohortDiagnosticsSettings(...)
    on.exit(DatabaseConnector::disconnect(cdSettings$getConnection()))
  }

  connection <- cdSettings$getConnection()

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "TimeDynamicNetwork.sql",
                                           packageName = utils::packageName(),
                                           dbms = DatabaseConnector::dbms(connection),
                                           time_bin_size = timeBinSize,
                                           cohort_ids = cdSettings$cohortIds,
                                           cohort_table_name = cdSettings$cohortTable,
                                           cohort_database_schema = cdSettings$cohortDatabaseSchema,
                                           cdm_database_schema = cdSettings$cdmDatabaseSchema)

  DatabaseConnector::executeSql(cdSettings$getConnection(), sql)

  # TODO: this is a quick logic hack - this will not be released
  writeCallback <- function(csvFile) {
    callback <- function(x, pos) {
      readr::write_csv(x, file.path(cdSettings$exportFolder, csvFile), append = pos != 1)
      NULL
    }
    return(callback)
  }


  # {DEFAULT @result_table = #time_dynamic_network}
  # {DEFAULT @co_occurrence_result_table = #co_occurrence_network}
  coNetworkRes <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #co_occurrence_network")
  tdNetworkRes <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM #time_dynamic_network")
  DatabaseConnector::renderTranslateQueryApplyBatched(connection,
                                                      "SELECT * FROM #co_occurrence_network",
                                                      fun = writeCallback("cd_time_dynamic_network.csv"))

  DatabaseConnector::renderTranslateQueryApplyBatched(connection,
                                                    "SELECT * FROM #co_occurrence_network",
                                                    fun = writeCallback("cd_occurrence_network.csv"))

  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               "DROP TABLE #co_occurrence_network; DROP TABLE #co_occurrence_network;")
  return(cdSettings)
}