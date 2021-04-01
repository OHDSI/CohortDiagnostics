# this script enables to easily remove log entries in incremental mode.
# if we want to rerun/overwrite previous data

library(magrittr)
logFolder <-
  ""
diagnosticsFileName <- "CreatedDiagnostics.csv"

listFiles <-
  list.files(
    path = logFolder,
    pattern = diagnosticsFileName,
    full.names = TRUE,
    recursive = TRUE
  )

# "getCohortCounts", "runInclusionStatistics", "runIncludedSourceConcepts",
# "runBreakdownIndexEvents", "runOrphanConcepts", "runTimeDistributions",
# "runVisitContext", "runIncidenceRate", "runCohortOverlap",
# "runCohortCharacterization", "runTemporalCohortCharacterization"


# tasksToRemove <- c("runTemporalCohortCharacterization", "runCohortCharacterization")




for (i in (1:length(listFiles))) {
  readr::read_csv(
    file = listFiles[[i]],
    col_types = readr::cols(),
    guess_max = min(1e7)
  ) %>%
    dplyr::filter(!.data$task %in% tasksToRemove) %>%
    readr::write_excel_csv(file = listFiles[[i]])
}
