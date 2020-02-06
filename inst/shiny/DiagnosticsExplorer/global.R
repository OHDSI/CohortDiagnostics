if (!exists("shinySettings")) {
  shinySettings <- list(dataFolder = "C:/temp/exampleStudy")
}
dataFolder <- shinySettings$dataFolder

if (file.exists(file.path(dataFolder, "PreMerged.RData"))) {
  writeLines("Using merged data detected in data folder")
  load(file.path(dataFolder, "PreMerged.RData"))
} else {
  zipFiles <- list.files(dataFolder, pattern = ".zip", full.names = TRUE)
  
  loadFile <- function(file, folder, overwrite) {
    tableName <- gsub(".csv$", "", file)
    camelCaseName <- SqlRender::snakeCaseToCamelCase(tableName)
    data <- readr::read_csv(file.path(folder, file), col_types = readr::cols(), guess_max = 1e7, locale = readr::locale(encoding = "UTF-8"))
    colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
    
    if (!overwrite && exists(camelCaseName, envir = .GlobalEnv)) {
      existingData <- get(camelCaseName, envir = .GlobalEnv)
      data <- rbind(existingData, data)
    }
    assign(camelCaseName, data, envir = .GlobalEnv)
    
    invisible(NULL)
  }
  
  for (i in 1:length(zipFiles)) {
    tempFolder <- tempfile()
    dir.create(tempFolder)
    unzip(zipFiles[i], exdir = tempFolder)
    
    csvFiles <- list.files(tempFolder, pattern = ".csv")
    lapply(csvFiles, loadFile, folder = tempFolder, overwrite = (i == 1))
    
    unlink(tempFolder, recursive = TRUE)
  }
}

cohort <- unique(cohort)
covariate <- unique(covariate)
conceptSets <- unique(includedSourceConcept[, c("cohortId", "conceptSetId", "conceptSetName")])

