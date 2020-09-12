library(magrittr)

source("R/Tables.R")
source("R/Other.R")


if (!exists("shinySettings")) {
  if (file.exists("data")) {
    shinySettings <- list(dataFolder = "data")
  } else {
    shinySettings <- list(dataFolder = "c:/temp/exampleStudy")
  }
}
dataFolder <- shinySettings$dataFolder

suppressWarnings(
  rm(
    "analysisRef",
    "temporalAnalysisRef",
    "temporalTimeRef",
    "covariateRef",
    "temporarlCovariateRef",
    "concept",
    "vocabulary",
    "domain",
    "conceptAncestor",
    "conceptRelationship",
    "cohort",
    "cohortCount",
    "cohortOverlap",
    "conceptSets",
    "database",
    "incidenceRate",
    "includedSourceConcept",
    "inclusionRuleStats",
    "indexEventBreakdown",
    "orphanConcept",
    "timeDistribution"
  )
)

if (file.exists(file.path(dataFolder, "PreMerged.RData"))) {
  writeLines("Using merged data detected in data folder")
  load(file.path(dataFolder, "PreMerged.RData"))
} else {
  zipFiles <- list.files(dataFolder, pattern = ".zip", full.names = TRUE)
  
  loadFile <- function(file, folder, overwrite) {
    # print(file)
    tableName <- gsub(".csv$", "", file)
    camelCaseName <- SqlRender::snakeCaseToCamelCase(tableName)
    data <- readr::read_csv(file.path(folder, file), 
                            col_types = readr::cols(), 
                            guess_max = min(1e7), 
                            locale = readr::locale(encoding = "UTF-8"))
    colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
    
    if (!overwrite && exists(camelCaseName, envir = .GlobalEnv)) {
      existingData <- get(camelCaseName, envir = .GlobalEnv)
      if (nrow(existingData) > 0) {
        if (nrow(data) > 0) {
          if (all(colnames(existingData) %in% colnames(data)) &&
              all(colnames(data) %in% colnames(existingData))) {
            data <- data[, colnames(existingData)]
          } else {
            stop("Table columns do no match previously seen columns. Columns in ", 
                 file, 
                 ":\n", 
                 paste(colnames(data), collapse = ", "), 
                 "\nPrevious columns:\n",
                 paste(colnames(existingData), collapse = ", "))
          }
        }
      }
      data <- rbind(existingData, data)
    }
    assign(camelCaseName, data, envir = .GlobalEnv)
    
    invisible(NULL)
  }
  
  for (i in 1:length(zipFiles)) {
    writeLines(paste("Processing", zipFiles[i]))
    tempFolder <- tempfile()
    dir.create(tempFolder)
    unzip(zipFiles[i], exdir = tempFolder)
    
    csvFiles <- list.files(tempFolder, pattern = ".csv")
    lapply(csvFiles, loadFile, folder = tempFolder, overwrite = (i == 1))
    
    unlink(tempFolder, recursive = TRUE)
  }
}

cohort <- cohort %>% 
  dplyr::distinct() %>% 
  dplyr::select(.data$cohortName, .data$cohortId) %>% 
  dplyr::arrange(.data$cohortName, .data$cohortId)

database <- database %>% 
  dplyr::distinct() %>%
  dplyr::mutate(databaseName = dplyr::case_when(is.na(.data$databaseName) ~ .data$databaseId, 
                                                TRUE ~ as.character(.data$databaseId)),
                description = dplyr::case_when(is.na(.data$description) ~ .data$databaseName,
                                               TRUE ~ as.character(.data$description))) %>% 
  dplyr::arrange(.data$databaseId)

if (exists("covariateRef")) {
  covariate <- covariateRef %>% 
    dplyr::group_by(.data$covariateId) %>% 
    dplyr::slice(1) %>% 
    dplyr::distinct() %>% 
    dplyr::arrange(.data$covariateName)
}

if (exists("temporalCovariateValue")) {
  temporalCovariate <- temporalCovariateRef %>% 
    dplyr::group_by(.data$covariateId) %>% 
    dplyr::slice(1) %>% 
    dplyr::distinct() %>% 
    tidyr::crossing(temporalTimeRef) %>% 
    dplyr::arrange(.data$covariateName, .data$timeId)
  
  temporalCovariateChoices <- temporalCovariate %>%
    dplyr::select(.data$timeId, .data$startDay, .data$endDay) %>%
    dplyr::distinct() %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId)
}

if (exists("includedSourceConcept")) {
  conceptSet <- includedSourceConcept %>%
    dplyr::select(.data$cohortId, 
                  .data$conceptSetId) %>% 
    dplyr::distinct() %>% 
    dplyr::left_join(conceptSets) %>%
    dplyr::arrange(.data$cohortId, .data$conceptSetName)
} else if (exists("orphanConcept")) {
  conceptSet <- orphanConcept %>%
    dplyr::select(.data$cohortId, 
                  .data$conceptSetId) %>%  
    dplyr::distinct() %>% 
    dplyr::left_join(conceptSets) %>%
    dplyr::arrange(.data$cohortId, .data$conceptSetName)
} else {
  conceptSet <- NULL 
}


if ("phenotypeDescription.csv" %in% list.files(path = dataFolder)) {
  print("loading phenotypeDescription and cohortDescription from local folder. App set to work in Phenotype library mode.")
  appTitle <- "Phenotype Library"
  cohortDescription <- readr::read_csv(file.path(dataFolder, 'cohortDescription.csv'), 
                                       col_types = readr::cols(), 
                                       guess_max = min(1e7), 
                                       locale = readr::locale(encoding = "UTF-8"),
                                       trim_ws = TRUE, 
                                       na = '0', 
                                       skip_empty_rows = TRUE) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = ''))) %>% 
    dplyr::arrange(.data$phenotypeId, .data$cohortDefinitionName)
  
  cohort <- cohort %>%
    dplyr::rename(cohortFullNameOld = .data$cohortFullName) %>% 
    dplyr::left_join(y = cohortDescription %>% 
                       dplyr::mutate(cohortId = utils::type.convert(.data$atlasId),
                                     cohortFullName = .data$cohortDefinitionName) %>% 
                       dplyr::select(.data$cohortId, .data$cohortFullName)) %>% 
    dplyr::relocate(.data$cohortFullName) %>% 
    dplyr::mutate(cohortFullName = dplyr::case_when(is.na(.data$cohortFullName) ~ .data$cohortFullNameOld,
                                                    TRUE ~ .data$cohortFullName)) %>% 
    dplyr::select(-.data$cohortFullNameOld) %>% 
    dplyr::arrange(.data$cohortFullName)
  
  phenotypeDescription <- readr::read_csv(file.path(dataFolder, "phenotypeDescription.csv"), 
                                          col_types = readr::cols(), 
                                          guess_max = min(1e7), 
                                          locale = readr::locale(encoding = "UTF-8"),
                                          trim_ws = TRUE, 
                                          na = '0', 
                                          skip_empty_rows = TRUE) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = ''))) %>% 
    dplyr::arrange(.data$phenotypeName, .data$phenotypeId)
} else {
  print("phenotypeDescription not found. App set to work in Cohort Diagnostics mode.")
}