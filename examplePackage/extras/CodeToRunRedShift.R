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
library(examplePackage)

path <- 'D:/yourStudyFolderRs'

# Optional: specify where the temporary files will be created:
options(andromedaTempFolder = file.path(path, "andromedaTemp"))

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()


# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                server = Sys.getenv("REDSHIFT_MDCD_SERVER"),
                                                                user = Sys.getenv("userSecureAWS"),
                                                                password = Sys.getenv("passwordSecureAWS"),
                                                                port = Sys.getenv("REDSHIFT_MDCD_PORT"))

# For Oracle: define a schema that can be used to emulate temp tables:
oracleTempSchema <- NULL

# Details specific to the database:
outputFolder <- file.path(path, "output")
cdmDatabaseSchema <- Sys.getenv("REDSHIFT_MDCD_CDM")
cohortDatabaseSchema <- Sys.getenv("REDSHIFT_MDCD_SCRATCH")
cohortTable <- "yourCohortTable"
databaseId <- "MDCD"
databaseName <- "IBM Medicaid database"
databaseDescription <- "IBM Medicaid database."

# Use this to run the cohorttDiagnostics. The results will be stored in the diagnosticsExport subfolder of the outputFolder. This can be shared between sites.
examplePackage::runCohortDiagnostics(connectionDetails = connectionDetails,
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
# Optional: if there are results zip files from multiple sites in a folder, this merges them, which will speed up starting the viewer:
CohortDiagnostics::preMergeDiagnosticsFiles(file.path(outputFolder, "diagnosticsExport"))

# Use this to view the results. Multiple zip files can be in the same folder. If the files were pre-merged, this is automatically detected: 
CohortDiagnostics::launchDiagnosticsExplorer(file.path(outputFolder, "diagnosticsExport"))


# To explore a specific cohort in the local database, viewing patient profiles:
CohortDiagnostics::launchCohortExplorer(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        cohortId = 123)
# Where 123 is the ID of the cohort you wish to inspect.
