library(magrittr)

source("R/Plots.R")
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

suppressWarnings(rm("cohort", "cohortCount", "cohortOverlap", "conceptSets", "database", "incidenceRate", "includedSourceConcept", "inclusionRuleStats", "indexEventBreakdown", "orphanConcept", "timeDistribution"))

if (file.exists(file.path(dataFolder, "PreMerged.RData"))) {
  writeLines("Using merged data detected in data folder")
  load(file.path(dataFolder, "PreMerged.RData"))
} else {
  zipFiles <- list.files(dataFolder, pattern = ".zip", full.names = TRUE)
  
  loadFile <- function(file, folder, overwrite) {
    # print(file)
    tableName <- gsub(".csv$", "", file)
    camelCaseName <- SqlRender::snakeCaseToCamelCase(tableName)
    data <- readr::read_csv(file.path(folder, file), col_types = readr::cols(), guess_max = 1e7, locale = readr::locale(encoding = "UTF-8"))
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
          dplyr::select(.data$cohortFullName, .data$cohortId, .data$cohortName) %>% 
          dplyr::arrange(.data$cohortFullName, .data$cohortId)

database <- database %>% 
            dplyr::distinct() %>% 
            dplyr::arrange(.data$databaseId)

if (exists("covariate")) {
  covariate <- covariate %>% 
    dplyr::distinct() %>% 
    dplyr::arrange(.data$covariateName)
  if (!"conceptId" %in% colnames(covariate)) {
    warning("conceptId not found in covariate file. Calculating conceptId from covariateId. This may rarely cause errors.")
  covariate <- covariate %>% 
    dplyr::mutate(conceptId = (.data$covariateId - .data$covariateAnalysisId)/1000)
  }
}

if (exists("temporalCovariate")) {
  temporalCovariate <- temporalCovariate %>% 
    dplyr::distinct() %>% 
    dplyr::arrange(.data$covariateName, .data$timeId)
  if (!"conceptId" %in% colnames(temporalCovariate)) {
    warning("conceptId not found in temporalCovariate file. Calculating conceptId from covariateId. This may rarely cause errors.")
    temporalCovariate <- temporalCovariate %>% 
      dplyr::mutate(conceptId = (.data$covariateId - .data$covariateAnalysisId)/1000)
  }
  temporalCovariateChoices <- temporalCovariate %>%
    dplyr::select(.data$timeId, .data$startDayTemporalCharacterization, .data$endDayTemporalCharacterization) %>%
    dplyr::distinct() %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDayTemporalCharacterization, " to end ", .data$endDayTemporalCharacterization)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId)
}

if (exists("includedSourceConcept")) {
  conceptSets <- includedSourceConcept %>% 
                  dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptSetName) %>% 
                  dplyr::distinct() %>% 
                  dplyr::arrange(.data$cohortId, .data$conceptSetName)
} else if (exists("orphanConcept")) {
  conceptSets <- orphanConcept %>% 
                  dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptSetName) %>% 
                  dplyr::distinct() %>% 
                  dplyr::arrange(.data$cohortId, .data$conceptSetName)
} else {
  conceptSets <- NULL 
}


if ("phenotypeDescription.csv" %in% list.files(path = dataFolder)) {
  print("loading phenotypeDescription and cohortDescription from local folder. App set to work in Phenotype library mode.")
  appTitle <- "Phenotype Library"
  cohortDescription <- readr::read_csv(file.path(dataFolder, 'cohortDescription.csv'), 
                                       col_types = readr::cols(), 
                                       guess_max = 1e7, 
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
                                          guess_max = 1e7, 
                                          locale = readr::locale(encoding = "UTF-8"),
                                          trim_ws = TRUE, 
                                          na = '0', 
                                          skip_empty_rows = TRUE) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = ''))) %>% 
    dplyr::arrange(.data$phenotypeName, .data$phenotypeId)
} else {
  print("phenotypeDescription not found. App set to work in Cohort Diagnostics mode.")
}
