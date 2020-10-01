library(magrittr)

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

rm("visitContext")



if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- temporalTimeRef %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}



# 
# if (is.null(connection)) {
#   if (!exists("shinySettings")) {
#     if (file.exists("data")) {
#       shinySettings <- list(dataFolder = "data")
#     } else {
#       shinySettings <- list(dataFolder = "S:/examplePackageOutput")
#     }
#     dataFolder <- shinySettings$dataFolder
#     if (file.exists(file.path(dataFolder, "PreMerged.RData"))) {
#       writeLines(paste0("Using merged data detected in folder '", dataFolder, "'"))
#       load(file.path(dataFolder, "PreMerged.RData"))
#     } else {
#       stop("No premerged file found")
#     }
#   }
# } else {
#   writeLines(paste0("Retrieving some tables from databse "))
#   allTables <- DatabaseConnector::getTableNames(connection = connection,
#                                                 databaseSchema = resultsDatabaseSchema)
#   globalTables <- c('analysis_ref', 'temporal_time_ref', 'cohort', 'cohort_count',
#                     'concept_sets', 'database', 'phenotype_description')
#   for (i in (1:length(globalTables))) {
#     assign(SqlRender::snakeCaseToCamelCase(globalTables[[i]]), 
#            queryAllData(connection = connection,
#                         databaseSchema = resultsDatabaseSchema,
#                         tableName = globalTables[[i]]))
#   }
#   blankTables <- setdiff(x = allTables, y = globalTables)
#   for (i in (1:length(blankTables))) {
#     assign(SqlRender::snakeCaseToCamelCase(blankTables[[i]]),
#            tidyr::tibble())
#   }
# }