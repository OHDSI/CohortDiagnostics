# this script enables to easily remove log entries in incremental mode.
# if we want to rerun/overwrite previous data

library(magrittr)
logFolder <-
  "D:\\studyResults\\SkeletonCohortDiagnosticsStudyP"
diagnosticsFileName <- "CreatedDiagnostics.csv"

listFiles <-
  list.files(
    path = logFolder,
    pattern = diagnosticsFileName,
    full.names = TRUE,
    recursive = TRUE
  )

# "getCohortCounts", "runInclusionStatistics", "runConceptSetDiagnostics", 
# "runVisitContext", "runIncidenceRate", "runCohortRelationship",
# "runCohortCharacterization", "runTemporalCohortCharacterization", "runCohortTimeSeries"


tasksToRemove <- c("runCohortRelationship")




for (i in (1:length(listFiles))) {
  readr::read_csv(
    file = listFiles[[i]],
    col_types = readr::cols(),
    guess_max = min(1e7),
    lazy = FALSE 
  ) %>%
    dplyr::filter(!.data$task %in% tasksToRemove) %>%
    readr::write_excel_csv(file = listFiles[[i]])
}

# 
# filesToDelete <- "cohort_relationships.csv"
# listFilesDelete <-
#   list.files(
#     path = logFolder,
#     pattern = filesToDelete,
#     full.names = TRUE,
#     recursive = TRUE
#   )
# for (i in (1:length(listFilesDelete))) {
#   unlink(listFilesDelete[[i]], recursive = TRUE, force = TRUE)
# }
