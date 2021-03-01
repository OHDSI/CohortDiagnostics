# Make sure to install all dependencies (not needed if already done):
# install.packages("SqlRender")
# install.packages("DatabaseConnector")
# install.packages("ggplot2")
# install.packages("ParallelLogger")
# install.packages("readr")
# install.packages("tibble")
# install.packages("dplyr")
# install.packages("RJSONIO")
# install.packages("devtools")
# devtools::install_github("FeatureExtraction")
# devtools::install_github("ROhdsiWebApi")
# devtools::install_github("CohortDiagnostics")


# Load the package
library(examplePhenotypeLibraryPackage)

# Optional: specify where the temporary files will be created:
options(andromedaTempFolder = "s:/andromedaTemp")

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()


# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
                                                                server = Sys.getenv("PDW_SERVER"),
                                                                user = NULL,
                                                                password = NULL,
                                                                port = Sys.getenv("PDW_PORT"))

# For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL

# Provide data-base specific information here
outputFolder <- "s:/examplePhenotypeLibraryPackageOutput/ccae"
cdmDatabaseSchema <- "CDM_IBM_CCAE_V1247.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "mschuemi_skeleton_ccae"
databaseId <- "CCAE"
databaseName <- "IBM MarketScan Commercial Claims and Encounters Database"
databaseDescription <- "IBM MarketScan® Commercial Claims and Encounters Database (CCAE) represent data from individuals enrolled in United States employer-sponsored insurance health plans. The data includes adjudicated health insurance claims (e.g. inpatient, outpatient, and outpatient pharmacy) as well as enrollment data from large employers and health plans who provide private healthcare coverage to employees, their spouses, and dependents. Additionally, it captures laboratory tests for a subset of the covered lives. This administrative claims database includes a variety of fee-for-service, preferred provider organizations, and capitated health plans." 

# Use this to run the cohortDiagnostics. The results will be stored in the diagnosticsExport subfolder of the outputFolder. This can be shared between sites.
examplePhenotypeLibraryPackage::runCohortDiagnostics(connectionDetails = connectionDetails,
                                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                                     cohortTable = cohortTable,
                                                     oracleTempSchema = oracleTempSchema,
                                                     outputFolder = outputFolder,
                                                     databaseId = databaseId,
                                                     databaseName = databaseName,
                                                     databaseDescription = databaseDescription,
                                                     createCohorts = TRUE,
                                                     runInclusionStatistics = TRUE,
                                                     runIncludedSourceConcepts = TRUE,
                                                     runOrphanConcepts = TRUE,
                                                     runTimeDistributions = TRUE,
                                                     runBreakdownIndexEvents = TRUE,
                                                     runIncidenceRates = TRUE,
                                                     runCohortOverlap = TRUE,
                                                     runCohortCharacterization = TRUE,
                                                     runTemporalCohortCharacterization = TRUE,
                                                     minCellCount = 5)

# To view the results:
CohortDiagnostics::preMergeDiagnosticsFiles(file.path(outputFolder, "diagnosticsExport"))

CohortDiagnostics::launchDiagnosticsExplorer(file.path(outputFolder, "diagnosticsExport"))
