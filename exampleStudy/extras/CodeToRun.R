library(exampleStudy)

# Optional: specify where the temporary files (used by the ff package) will be created:
options(fftempdir = "s:/FFtemp")

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

# For MDCD:
outputFolder <- "s:/exampleStudy/mdcd"
cdmDatabaseSchema <- "cdm_ibm_mdcd_v1023.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "mschuemi_skeleton_mdcd"
databaseId <- "MDCD"
databaseName <- "Truven Health MarketScan® Multi-State Medicaid Database"
databaseDescription <- "Truven Health MarketScan® Multi-State Medicaid Database (MDCD) adjudicated US health insurance claims for Medicaid enrollees from multiple states and includes hospital discharge diagnoses, outpatient diagnoses and procedures, and outpatient pharmacy claims as well as ethnicity and Medicare eligibility. Members maintain their same identifier even if they leave the system for a brief period however the dataset lacks lab data. [For further information link to RWE site for Truven MDCD."

# For Synpuf:
outputFolder <- "s:/exampleStudy/synpuf"
cdmDatabaseSchema <- "cdm_synpuf_v667.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "mschuemi_skeleton_synpuf"
databaseId <- "Synpuf"
databaseName <- "Medicare Claims Synthetic Public Use Files (SynPUFs)"
databaseDescription <- "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information."

runFeasibility(connectionDetails = connectionDetails,
               cdmDatabaseSchema = cdmDatabaseSchema,
               cohortDatabaseSchema = cohortDatabaseSchema,
               cohortTable = cohortTable,
               oracleTempSchema = oracleTempSchema,
               outputFolder = outputFolder,
               databaseId = databaseId,
               databaseName = databaseName,
               databaseDescription = databaseDescription,
               createCohorts = TRUE,
               runDiagnostics = TRUE,
               exportDiagnostics = TRUE,
               minCellCount = 5)


CohortDiagnostics::launchDiagnosticsExplorer(file.path(outputFolder, "feasibilityExport"))

CohortDiagnostics::launchCohortExplorer(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        cohortId = 1770710)
