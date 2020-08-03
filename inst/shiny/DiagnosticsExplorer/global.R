library(magrittr)

cohortBaseUrl <- "https://atlas.ohdsi.org/#/cohortdefinition/"
conceptBaseUrl <- "https://athena.ohdsi.org/search-terms/terms/"


if (!exists("shinySettings")) {
  if (file.exists("data")) {
    shinySettings <- list(dataFolder = "data")
  } else {
    shinySettings <- list(dataFolder = "c:/temp/exampleStudy")
  }
}
dataFolder <- shinySettings$dataFolder

if ("phenotypeDescription.csv" %in% list.files(path = dataFolder)) {
  print("loading phenotypeDescription and cohortDescription from local folder")
  phenotypeDescription <- readr::read_csv(file.path(dataFolder, "phenotypeDescription.csv"), 
                                          col_types = readr::cols(), 
                                          guess_max = 1e7, 
                                          locale = readr::locale(encoding = "UTF-8"),
                                          trim_ws = TRUE) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
  
  cohortDescription <- readr::read_csv(file.path(dataFolder, 'cohortDescription.csv'), 
                                       col_types = readr::cols(), 
                                       guess_max = 1e7, 
                                       locale = readr::locale(encoding = "UTF-8"),
                                       trim_ws = TRUE) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
  
} else if (system.file('phenotypeLibrary', 'phenotypeDescription.csv', package = 'phenotypeLibrary') != '') {
  print("loading phenotypeDescription and cohortDescription from phenotype library package")
  phenotypeDescription <- readr::read_csv(file.path(system.file('phenotypeLibrary', 'phenotypeDescription.csv', package = 'phenotypeLibrary')), 
                                          col_types = readr::cols(), 
                                          guess_max = 1e7, 
                                          locale = readr::locale(encoding = "UTF-8"),
                                          trim_ws = TRUE
  ) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
  
  cohortDescription <- readr::read_csv(file.path(system.file('phenotypeLibrary', 'cohortDescription.csv', package = 'phenotypeLibrary')), 
                                       col_types = readr::cols(), 
                                       guess_max = 1e7, 
                                       locale = readr::locale(encoding = "UTF-8"),
                                       trim_ws = TRUE
  ) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
}

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
          dplyr::select(.data$cohortFullName, .data$cohortId, .data$cohortName)

if (exists("covariate")) {
  covariate <- covariate %>% 
    dplyr::distinct()
  if (!"conceptId" %in% colnames(covariate)) {
    warning("conceptId not found in covariate file. Calculating conceptId from covariateId. This may rarely cause errors.")
  covariate <- covariate %>% 
    dplyr::mutate(conceptId = (.data$covariateId - .data$covariateAnalysisId)/1000)
  }
}

if (exists("temporalCovariate")) {
  temporalCovariate <- temporalCovariate %>% 
    dplyr::distinct()
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
                  dplyr::distinct()
} else if (exists("orphanConcept")) {
  conceptSets <- orphanConcept %>% 
                  dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptSetName) %>% 
                  dplyr::distinct()
} else {
  conceptSets <- NULL 
}

