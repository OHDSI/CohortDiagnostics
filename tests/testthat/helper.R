#' utility function to make sure connection is closed after usage
with_dbc_connection <- function(connection, code) {
  on.exit({
    DatabaseConnector::disconnect(connection)
  })
  eval(substitute(code), envir = connection, enclos = parent.frame())
}

#' Only works with postgres > 9.4
.pgTableExists <- function(connection, schema, tableName) {
  return(!is.na(
    DatabaseConnector::renderTranslateQuerySql(
      connection,
      "SELECT to_regclass('@schema.@table');",
      table = tableName,
      schema = schema
    )
  )[[1]])
}

getDefaultSubsetDefinition <- function() {
  CohortGenerator::createCohortSubsetDefinition(
    name = "subsequent GI bleed with 365 days prior observation",
    definitionId = 1,
    subsetOperators = list(
      # First subset to only those patients in cohort 14909 (GI bleed) within 30 days of cohort start
      CohortGenerator::createCohortSubsetOperator(
        name = "with GI bleed within 30 days of cohort start",
        cohortIds = 14909,
        cohortCombinationOperator = "any",
        negate = FALSE,
        windows = list(
          CohortGenerator::createSubsetCohortWindow(
            startDay = 0,
            endDay = 30,
            targetAnchor = "cohortStart"
          ),
          CohortGenerator::createSubsetCohortWindow(
            startDay = 0,
            endDay = 9999999,
            targetAnchor = "cohortStart"
          )
        )
      ),
      # Next, subset to only those with 365 days of prior observation
      CohortGenerator::createLimitSubsetOperator(
        name = "Observation of at least 365 days prior",
        priorTime = 365,
        followUpTime = 0,
        limitTo = "firstEver"
      )
    )
  )
}

# Create a cohort definition set from test cohorts
loadTestCohortDefinitionSet <- function(cohortIds = NULL, useSubsets = TRUE) {
  if (grepl("testthat", getwd())) {
    cohortPath <- "cohorts"
  } else {
    cohortPath <- file.path("tests", "testthat", "cohorts")
  }

  creationFile <- file.path(cohortPath, "CohortsToCreate.csv")
  suppressMessages({
    cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
      settingsFileName = creationFile,
      sqlFolder = cohortPath,
      jsonFolder = cohortPath,
      cohortFileNameValue = c("cohortId")
    )
  })
 
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- dplyr::filter(cohortDefinitionSet, .data$cohortId %in% cohortIds)
  }

  if (useSubsets) {
    cohortDefinitionSet <- CohortGenerator::addCohortSubsetDefinition(cohortDefinitionSet, getDefaultSubsetDefinition(), targetCohortIds = c(18345))
  }

  cohortDefinitionSet$checksum <- CohortGenerator::computeChecksum(cohortDefinitionSet$sql)

  return(dplyr::tibble(cohortDefinitionSet))
}

#' Use to create test fixture for shiny tests
#' Required when data model changes
createTestShinyDb <- function(connectionDetails = Eunomia::getEunomiaConnectionDetails(),
                              cohortDefinitionSet = loadTestCohortDefinitionSet(),
                              outputPath = "inst/shiny/DiagnosticsExplorer/tests/testDb.sqlite",
                              cohortTable = "cohort",
                              vocabularyDatabaseSchema = "main",
                              cohortDatabaseSchema = "main",
                              cdmDatabaseSchema = "main",
                              ...) {
  folder <- tempfile()
  dir.create(folder)
  on.exit(unlink(folder, recursive = TRUE, force = TRUE))

  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = FALSE
  )

  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    exportFolder = file.path(folder, "export"),
    databaseId = "Eunomia",
    incremental = FALSE,
    ...
  )

  createMergedResultsFile(
    dataFolder = file.path(folder, "export"),
    sqliteDbPath = outputPath,
    overwrite = TRUE
  )
}



createCustomCdm <- function(jsonDataFilePath) {
  connectionDetails <- tryCatch(
    Eunomia::getEunomiaConnectionDetails(),
    error = function(e) {
      message("Eunomia data not available (getEunomiaConnectionDetails/loadDataFiles failed). ",
              "Use downloadEunomiaData() or set EUNOMIA_DATA_FOLDER. Error: ", conditionMessage(e))
      return(NULL)
    }
  )
  if (is.null(connectionDetails)) {
    return(NULL)
  }

  tablesToTruncate <- c("person", "observation_period", "visit_occurrence", "visit_detail", 
                        "condition_occurrence", "drug_exposure", "procedure_occurrence", 
                        "device_exposure", "measurement", "observation", "death", "note", 
                        "note_nlp", "specimen", "fact_relationship", "location", "care_site", 
                        "provider", "payer_plan_period", "cost", "drug_era", "dose_era", 
                        "condition_era", "metadata", "cdm_source", "cohort_definition", 
                        "attribute_definition")
  
  connection <- DatabaseConnector::connect(connectionDetails)
  
  # remove tables that are not vocabulary tables
  for (tbl in tablesToTruncate) {
    DatabaseConnector::executeSql(connection, paste("delete from ", tbl, ";"), progressBar = FALSE)
  }
  
  jsonData <- RJSONIO::fromJSON(system.file(jsonDataFilePath, package = "CohortDiagnostics"))
  
  # Convert the JSON data into a data frame and append it to the blank CDM
  for (tableName in names(jsonData)) {
    
    patientData <- dplyr::bind_rows(lapply(jsonData[[tableName]], as.data.frame)) %>%
      dplyr::mutate(dplyr::across(dplyr::matches("date$"), ~as.Date(.))) %>%
      dplyr::mutate(dplyr::across(dplyr::matches("datetime$"),  ~as.POSIXct(., format = "%Y-%m-%d %H:%M:%S", tz = "UTC")))

    DatabaseConnector::dbAppendTable(connection, tableName, patientData)
  }

  message("Patients pushed to blank CDM successfully")
  
  return(connectionDetails)
  
}


addCohortTable <- function(connection, cohortDataFilePath){
  
  cohortTableData <- readr::read_csv(cohortDataFilePath, col_types = c("iiDD"))
  
  cohortTableData <- cohortTableData %>% 
    mutate(across(ends_with("DATE"), ~ as.Date(.x, format = "%Y-%m-%d")))
  
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "cohort",
                                 data = cohortTableData
  )
}

getUniqueTempDir <- function(){
  random_string <- paste(sample(c(letters, LETTERS, 0:9), 10, replace = TRUE), collapse = "")
  uniqueComponent <- as.integer(Sys.time()) %% 10000
  tempDir <- file.path(tempdir(), paste0(uniqueComponent, random_string, "exp"))
  
  return(tempDir)
}

#' Create an in-memory DuckDB database with minimal CDM tables and fixture data for SQL unit tests.
#' DuckDB has native DATE type and runs in-memory. Returns connection and server-like list.
#' @param observationPeriodOnly If TRUE, only create observation_period (for GetCalendarYearRange, ObservedPerCalendarMonth).
#' @param emptyObservationPeriod If TRUE (only when observationPeriodOnly=TRUE), create observation_period with 0 rows.
#' @param fullCdm If TRUE, create person, observation_period, concept, vocabulary, cohort, visit_occurrence, and empty domain tables.
#' @return list with connectionDetails, connection, path (NULL for in-memory), cdmDatabaseSchema, cohortDatabaseSchema, vocabularyDatabaseSchema, tempEmulationSchema, cohortTable, cohortIds, cohortDefinitionSet (when fullCdm=TRUE).
createSqlTestFixtureDb <- function(observationPeriodOnly = FALSE, emptyObservationPeriod = FALSE, fullCdm = FALSE) {
  path <- tempfile(fileext = ".duckdb")
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "duckdb", server = path)
  con <- DatabaseConnector::connect(connectionDetails)
  # Use file-based DuckDB so tests that open a second connection (e.g. createResultsDataModel) see the same DB.

  if (observationPeriodOnly || fullCdm) {
    DatabaseConnector::executeSql(con, "
      CREATE TABLE observation_period (
        observation_period_id INTEGER PRIMARY KEY,
        person_id INTEGER NOT NULL,
        observation_period_start_date DATE NOT NULL,
        observation_period_end_date DATE NOT NULL,
        period_type_concept_id INTEGER
      );")
    if (!emptyObservationPeriod) {
      DatabaseConnector::executeSql(con, "
        INSERT INTO observation_period (observation_period_id, person_id, observation_period_start_date, observation_period_end_date)
        VALUES (1, 1, DATE '2020-06-01', DATE '2022-11-15');")
    }
  }

  if (fullCdm) {
    DatabaseConnector::executeSql(con, "
      CREATE TABLE person (
        person_id INTEGER PRIMARY KEY,
        gender_concept_id INTEGER,
        year_of_birth INTEGER,
        race_concept_id INTEGER,
        ethnicity_concept_id INTEGER
      );
      INSERT INTO person (person_id, gender_concept_id, year_of_birth) VALUES (1, 8532, 1980);
      CREATE TABLE concept (
        concept_id INTEGER PRIMARY KEY,
        concept_name VARCHAR(255),
        vocabulary_id VARCHAR(50),
        standard_concept VARCHAR(1),
        invalid_reason VARCHAR(1)
      );
      INSERT INTO concept (concept_id, concept_name, vocabulary_id, standard_concept, invalid_reason) VALUES
        (8532, 'Female', 'Gender', 'S', NULL),
        (8507, 'Male', 'Gender', 'S', NULL);
      CREATE TABLE vocabulary (vocabulary_id VARCHAR(50) PRIMARY KEY, vocabulary_name VARCHAR(255));
      INSERT INTO vocabulary VALUES ('Gender', 'Gender');
      CREATE TABLE cohort (
        cohort_definition_id INTEGER,
        subject_id INTEGER,
        cohort_start_date DATE,
        cohort_end_date DATE
      );
      INSERT INTO cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
      VALUES (17492, 1, DATE '2020-06-01', DATE '2020-06-01');
      CREATE TABLE visit_occurrence (
        visit_occurrence_id INTEGER PRIMARY KEY,
        person_id INTEGER,
        visit_concept_id INTEGER,
        visit_source_concept_id INTEGER,
        visit_start_date DATE,
        visit_end_date DATE
      );
      INSERT INTO visit_occurrence (visit_occurrence_id, person_id, visit_concept_id, visit_source_concept_id, visit_start_date, visit_end_date)
      VALUES (1, 1, 9201, 0, DATE '2020-05-15', DATE '2020-05-20');
      CREATE TABLE condition_occurrence (condition_occurrence_id INTEGER, person_id INTEGER, condition_concept_id INTEGER, condition_source_concept_id INTEGER, condition_start_date DATE);
      CREATE TABLE drug_exposure (drug_exposure_id INTEGER, person_id INTEGER, drug_concept_id INTEGER, drug_source_concept_id INTEGER, drug_exposure_start_date DATE);
      CREATE TABLE procedure_occurrence (procedure_occurrence_id INTEGER, person_id INTEGER, procedure_concept_id INTEGER, procedure_source_concept_id INTEGER, procedure_date DATE);
      CREATE TABLE measurement (measurement_id INTEGER, person_id INTEGER, measurement_concept_id INTEGER, measurement_source_concept_id INTEGER, measurement_date DATE);
      CREATE TABLE observation (observation_id INTEGER, person_id INTEGER, observation_concept_id INTEGER, observation_source_concept_id INTEGER, observation_date DATE);
      CREATE TABLE concept_ancestor (ancestor_concept_id INTEGER, descendant_concept_id INTEGER);
      CREATE TABLE concept_relationship (concept_id_1 INTEGER, concept_id_2 INTEGER, relationship_id VARCHAR(50));
      CREATE TABLE concept_synonym (concept_id INTEGER, concept_synonym_name VARCHAR(1000));
    ")
    cohortDefinitionSet <- loadTestCohortDefinitionSet(c(17492L, 17493L), useSubsets = FALSE)
    if (!is.null(cohortDefinitionSet) && nrow(cohortDefinitionSet) > 0) {
      if (!"isSubset" %in% names(cohortDefinitionSet)) cohortDefinitionSet$isSubset <- FALSE
    }
  } else {
    cohortDefinitionSet <- NULL
  }

  out <- list(
    connectionDetails = connectionDetails,
    connection = con,
    path = path,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    tempEmulationSchema = NULL,
    cohortTable = "cohort",
    cohortIds = c(17492L, 17493L),
    cohortDefinitionSet = cohortDefinitionSet
  )
  return(out)
}




