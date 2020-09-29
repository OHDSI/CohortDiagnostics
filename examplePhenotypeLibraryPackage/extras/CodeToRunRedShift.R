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

path <- "s:/examplePhenotypeLibraryPackageOutput"

# Optional: specify where the temporary files will be created:
options(andromedaTempFolder = file.path(path, "andromedaTemp"))

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()
oracleTempSchema <- NULL

# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringJmdc"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringCcae"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))
outputFolder <- "s:/examplePhenotypeLibraryPackage/ccae"
cdmDatabaseSchema <- "cdm"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "mschuemi_skeleton_ccae"
databaseId <- "CCAE"
databaseName <- "IBM MarketScan Commercial Claims and Encounters Database"
databaseDescription <- "IBM MarketScan® Commercial Claims and Encounters Database (CCAE) represent data from individuals enrolled in United States employer-sponsored insurance health plans. The data includes adjudicated health insurance claims (e.g. inpatient, outpatient, and outpatient pharmacy) as well as enrollment data from large employers and health plans who provide private healthcare coverage to employees, their spouses, and dependents. Additionally, it captures laboratory tests for a subset of the covered lives. This administrative claims database includes a variety of fee-for-service, preferred provider organizations, and capitated health plans." 




# For Oracle: define a schema that can be used to emulate temp tables:


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringJmdc"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))


outputFolder <- file.path(path, "output")
cdmDatabaseSchema <- "cdm"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "cd_test"
databaseId <- "JMDC"
databaseName <- "Japan Medical Data Center"
databaseDescription <- "Japan Medical Data Center (JDMC) database consists of data from 60 Society-Managed Health Insurance plans covering workers aged 18 to 65 and their dependents (children younger than 18 years old and elderly people older than 65 years old). JMDC data includes membership status of the insured people and claims data provided by insurers under contract (e.g. patient-level demographic information, inpatient and outpatient data inclusive of diagnosis and procedures, and prescriptions as dispensed claims information). Claims data are derived from monthly claims issued by clinics, hospitals and community pharmacies; for claims only the month and year are provided however prescriptions, procedures, admission, discharge, and start of medical care as associated with a full date.\nAll diagnoses are coded using ICD-10. All prescriptions refer to national Japanese drug codes, which have been linked to ATC. Procedures are encoded using local procedure codes, which the vendor has mapped to ICD-9 procedure codes. The annual health checkups report a standard battery of measurements (e.g. BMI), which are not coded but clearly described."


# Use this to run the cohorttDiagnostics. The results will be stored in the diagnosticsExport subfolder of the outputFolder. This can be shared between sites.
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
