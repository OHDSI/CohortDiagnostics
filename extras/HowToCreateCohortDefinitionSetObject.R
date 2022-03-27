# Cohort Definition Set object ----
# What cohorts do you want to diagnose? The input for cohort diagnostics is a cohort definition set object. 


# There are two ways to create such object

## ROhdsiWebApi ----
# if your cohorts are in Atlas, then do the following:
listOfCohortIdInAtlas <- c(2, 3)
baseUrl <- "https://api.ohdsi.org/WebAPI"
ROhdsiWebApi::authorizeWebApi(baseUrl = baseUrl, authMethod = "windows")
cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(baseUrl = baseUrl, cohortIds = listOfCohortIdInAtlas, generateStats = TRUE)



## Package or file system ----
# if your cohorts are in an R-package
cohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) %>%  dplyr::tibble()

# for file system, please provide the full path of the 
