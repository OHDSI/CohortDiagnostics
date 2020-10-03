library(magrittr)

source("R/Private.R")
source("R/Tables.R")
source("R/Other.R")
source("R/Plots.R")
source("R/Results.R")

if (!exists("shinySettings")) {
  if (file.exists("data")) {
    shinySettings <- list(dataFolder = "data")
  } else {
    shinySettings <- list(dataFolder = "S:/examplePackageOutput")
  }
}
dataFolder <- shinySettings$dataFolder

suppressWarnings(
  rm(
    "analysisRef",
    "temporalAnalysisRef",
    "temporalTimeRef",
    "covariateValue",
    "covariateRef",
    "temporalCovariateValue",
    "temporalCovariateRef",
    "concept",
    "conceptSynonym",
    "vocabulary",
    "domain",
    "relationship",
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
  writeLines(paste0("Using merged data detected in folder '", dataFolder, "'"))
  load(file.path(dataFolder, "PreMerged.RData"))
} else {
  stop("No premerged file found")
}

if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- temporalTimeRef %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}




rm("visitContext")