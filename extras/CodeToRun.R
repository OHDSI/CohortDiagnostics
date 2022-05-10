# SETUP --------------------------------------------------------------------

# Pre-requisites ----
remotes::install_github('OHDSI/CohortGenerator')
remotes::install_github('OHDSI/CohortDiagnostics')

# Cohort Definition set ----
# Cohort Diagnostics requires as an input a Data.frame of cohorts must include columns cohortId, cohortName, json, sql
# there are multiple ways to generate it. To generate from Atlas - use ROhdsiWebApi::exportCohortDefinitionSett
# To generate from another R package or file system use CohortGenerator::getCohortDefinitionSet
# See script "HowToCreateCohortDefinitionSetObject.R"

remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')
cohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) %>%  dplyr::tibble()

# connection details ----
# Details for connecting to the server:
# See ?DatabaseConnector::createConnectionDetails for help
connectionDetails <-
  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = "some.server.com/ohdsi",
    user = "joe",
    password = "secret"
  )

outputFolder <- "D:/YourOutputLocation"
cdmDatabaseSchema <- "cdm_synpuf"
vocabularyDatabaseSchema <-
  cdmDatabaseSchema # maybe different in some instances. Please check with your administrator
cohortDatabaseSchema <- "scratch.dbo"
cohortTables <- CohortGenerator::getCohortTableNames()
databaseId <- "synpuf"
databaseName <-
  "Medicare Claims Synthetic Public Use Files (SynPUFs)"
databaseDescription <-
  "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information."

# Please delete previous content if needed
# unlink(x = outputFolder,
#        recursive = TRUE,
#        force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)


# Execution --------------------------------------------------------------------
## Create cohort tables on remote ----
CohortGenerator::createCohortTables(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames,
  incremental = TRUE
)
## Generate cohort on remote ----
CohortGenerator::generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet,
  cohortDatabaseSchema = cohortDatabaseSchema,
  incremental = TRUE,
  incrementalFolder = file.path(outputFolder, "incremental")
)

## Execute Cohort Diagnostics on remote ----
CohortDiagnostics::executeDiagnostics(
  cohortDefinitionSet = cohortDefinitionSet,
  exportFolder = outputFolder,
  databaseId = databaseId,
  databaseName = databaseName,
  databaseDescription = databaseDescription,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  connectionDetails = connectionDetails,
  cohortTableNames = cohortTableNames,
  vocabularyDatabaseSchema = vocabularyDatabaseSchema,
  minCellCount = 5,
  incremental = TRUE
)


# package results ----
CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
# Launch diagnostics explorer shiny app ----
CohortDiagnostics::launchDiagnosticsExplorer()
