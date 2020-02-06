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

# For CCAE:
outputFolder <- "s:/exampleStudy/ccae"
cdmDatabaseSchema <- "cdm_ibm_ccae_v1061.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "mschuemi_skeleton_ccae"
databaseId <- "CCAE"
databaseName <- "Truven Health MarketScan Commercial Claims and Encounters Database"
databaseDescription <- "Truven Health MarketScan® Commercial Claims and Encounters Database (CCAE) represent data from individuals enrolled in United States employer-sponsored insurance health plans. The data includes adjudicated health insurance claims (e.g. inpatient, outpatient, and outpatient pharmacy) as well as enrollment data from large employers and health plans who provide private healthcare coverage to employees, their spouses, and dependents. Additionally, it captures laboratory tests for a subset of the covered lives. This administrative claims database includes a variety of fee-for-service, preferred provider organizations, and capitated health plans."

# For JMDC:
outputFolder <- "s:/exampleStudy/jmdc"
cdmDatabaseSchema <- "cdm_ibm_mdcd_v995.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "mschuemi_skeleton_jmdc"
databaseId <- "JMDC"
databaseName <- "Japan Medical Data Center"
databaseDescription <- "Japan Medical Data Center (JDMC) database consists of data from 60 Society-Managed Health Insurance plans covering workers aged 18 to 65 and their dependents (children younger than 18 years old and elderly people older than 65 years old). JMDC data includes membership status of the insured people and claims data provided by insurers under contract (e.g. patient-level demographic information, inpatient and outpatient data inclusive of diagnosis and procedures, and prescriptions as dispensed claims information). Claims data are derived from monthly claims issued by clinics, hospitals and community pharmacies; for claims only the month and year are provided however prescriptions, procedures, admission, discharge, and start of medical care as associated with a full date.\nAll diagnoses are coded using ICD-10. All prescriptions refer to national Japanese drug codes, which have been linked to ATC. Procedures are encoded using local procedure codes, which the vendor has mapped to ICD-9 procedure codes. The annual health checkups report a standard battery of measurements (e.g. BMI), which are not coded but clearly described."

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
