# this script enables to easily remove log entries in incremental mode. 
# if we want to rerun/overwrite previous data


logFolder <- "C:\\data"
diagnosticsFileName <- "CreatedDiagnostics.csv"

# "getCohortCounts", "runInclusionStatistics", "runIncludedSourceConcepts", 
# "runBreakdownIndexEvents", "runOrphanConcepts", "runTimeDistributions", 
# "runVisitContext", "runIncidenceRate", "runCohortOverlap", 
# "runCohortCharacterization", "runTemporalCohortCharacterization"


# tasksToRemove <- c("runTemporalCohortCharacterization", "runCohortCharacterization")

filesWithDiagnosticsLog <- list.files(path = logFolder, 
                                      pattern = diagnosticsFileName, 
                                      full.names = TRUE, 
                                      recursive = TRUE)

for (i in (1:length(filesWithDiagnosticsLog))) {
  readr::read_csv(file = filesWithDiagnosticsLog[[i]],
                          col_types = readr::cols(), 
                          guess_max = min(1e7)) %>% 
    dplyr::filter(!.data$task %in% tasksToRemove) %>% 
    readr::write_excel_csv(path = filesWithDiagnosticsLog[[i]])
}