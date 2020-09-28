library(magrittr)

source("R/Tables.R")
source("R/Other.R")
source("R/Plots.R")
source("R/Results.R")

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
    "timeDistribution",
    "visitContext"
  )
)

if (file.exists(file.path(dataFolder, "PreMerged.RData"))) {
  writeLines("Using merged data detected in data folder")
  load(file.path(dataFolder, "PreMerged.RData"))
} else {
  writeLines("No premerged file found")
}

rm("visitContext")

database <- database %>% 
  dplyr::distinct() %>%
  dplyr::mutate(databaseName = dplyr::case_when(is.na(.data$databaseName) ~ .data$databaseId, 
                                                TRUE ~ as.character(.data$databaseId)),
                description = dplyr::case_when(is.na(.data$description) ~ .data$databaseName,
                                               TRUE ~ as.character(.data$description))) %>% 
  dplyr::arrange(.data$databaseId)

if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- temporalTimeRef %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}
