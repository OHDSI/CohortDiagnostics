importFromPhenotypeLibrary <- function(phenotypeLibraryFolder = "../PhenotypeLibrary") {
  phenotypeFolders <- list.files(file.path(phenotypeLibraryFolder, "inst"), pattern = "[0-9]+", full.names = TRUE)
  
  ParallelLogger::logInfo("Importing from ", length(phenotypeFolders), " phenotype folders")
  
  ParallelLogger::logInfo("- Importing phenotype descriptions to 'inst/settings/phenotypeDescription.csv'")
  phenotypeDescription <- lapply(file.path(phenotypeFolders, "phenotypeDescription.csv"), readr::read_csv, col_types = readr::cols())
  phenotypeDescription <- dplyr::bind_rows(phenotypeDescription)
  readr::write_csv(phenotypeDescription, "inst/settings/phenotypeDescription.csv")
  
  ParallelLogger::logInfo("- Importing cohort descriptions to 'inst/settings/cohortDescription.csv'")
  cohortDescription <- lapply(file.path(phenotypeFolders, "cohortDescription.csv"), readr::read_csv, col_types = readr::cols())
  cohortDescription <- dplyr::bind_rows(cohortDescription)
  readr::write_csv(cohortDescription, "inst/settings/cohortDescription.csv")
  ParallelLogger::logInfo("- Found ", nrow(cohortDescription), " cohort descriptions")
  
  # JSON
  ParallelLogger::logInfo("- Importing cohort JSON files to 'inst/cohorts'")
  unlink("inst/cohorts", recursive = TRUE)
  dir.create("inst/cohorts", recursive = TRUE)
  jsonFiles <- list.files(file.path(phenotypeLibraryFolder, "inst"), pattern = "[0-9]+.json", recursive = TRUE, full.names = TRUE)
  jsonFiles <- jsonFiles[grepl("[0-9]+/[0-9]+.json$", jsonFiles)]
  success <- all(file.copy(jsonFiles, file.path("inst/cohorts", basename(jsonFiles))))
  if (!success) {
    stop("Error copying JSON files")
  }
  
  # SQL
  ParallelLogger::logInfo("- Importing cohort SQL files to 'inst/sql/sql_server'")
  unlink("inst/sql/sql_server", recursive = TRUE)
  dir.create("inst/sql/sql_server", recursive = TRUE)
  sqlFiles <- list.files(file.path(phenotypeLibraryFolder, "inst"), pattern = "[0-9]+.sql", recursive = TRUE, full.names = TRUE)
  sqlFiles <- sqlFiles[grepl("[0-9]+/[0-9]+.sql$", sqlFiles)]
  success <- all(file.copy(sqlFiles, file.path("inst/sql/sql_server", basename(sqlFiles))))
  if (!success) {
    stop("Error copying SQL files")
  }

  ParallelLogger::logInfo("- Checking referential integrity")
  if (!setequal(phenotypeDescription$phenotypeId, unique(cohortDescription$phenotypeId))) {
    stop("Phenotype ID mismatch")
  }
  
  jsonCohortIds <- as.numeric(gsub(".json", "", basename(jsonFiles)))
  if (!setequal(jsonCohortIds, cohortDescription$cohortId)) {
    stop("Cohort ID mistmatch for JSON files")
  }
  
  sqlCohortIds <- as.numeric(gsub(".sql", "", basename(sqlFiles)))
  if (!setequal(sqlCohortIds, cohortDescription$cohortId)) {
    stop("Cohort ID mistmatch for SQL files")
  }
  ParallelLogger::logInfo("Done importing")
  invisible(NULL)
}