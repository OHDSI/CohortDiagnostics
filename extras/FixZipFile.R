# Code for fixing old zip files in a folder

oldDataFolder <- "C:/Users/MSCHUEMI/Desktop/Immunology"
newDataFolder <- "C:/Users/MSCHUEMI/Desktop/ImmunologyFixed"

# No changes beyond this point
dir.create(newDataFolder, recursive = TRUE)

unzipFolder <- tempfile("unzipTempFolder")
dir.create(path = unzipFolder, recursive = TRUE)

zipFiles <- list.files(oldDataFolder, pattern = ".zip", full.names = TRUE, recursive = TRUE)
for (zipFile in zipFiles) {
  writeLines(paste("Unzipping", zipFile))
  zip::unzip(zipFile, exdir = unzipFolder)
  
  writeLines("Fixing cohort table")
  cohort <- readr::read_csv(file.path(unzipFolder, "cohort.csv"),
                            col_types = readr::cols(),
                            guess_max = 1e6)  
  if ("referent_concept_id" %in% colnames(cohort) && !"phenotype_id" %in% colnames(cohort)) {
    cohort$phenotype_id <- round(cohort$cohort_id / 1000) * 1000
  }
  
  if ("referent_concept_id" %in% colnames(cohort)) {
    cohort$referent_concept_id <- NULL
  }
  readr::write_csv(cohort, file.path(unzipFolder, "cohort.csv"))
  
  writeLines("Zipping file")
  zip::zipr(file.path(newDataFolder, basename(zipFile)), list.files(unzipFolder, full.names = TRUE))
  unlink(list.files(unzipFolder, full.names = TRUE))  
}



