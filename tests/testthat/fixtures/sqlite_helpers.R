# SQLite Helper Functions for Tests
# Functions to create and populate in-memory SQLite databases for testing

#' Create an in-memory SQLite database connection
#'
#' @return A DatabaseConnector connection to an in-memory SQLite database
createInMemorySqliteDb <- function() {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = ":memory:"
  )
  
  connection <- DatabaseConnector::connect(connectionDetails)
  return(connection)
}

#' Populate minimal CDM schema in SQLite database
#'
#' @param connection Database connection
#' @return NULL (modifies database in place)
populateMinimalCdmSchema <- function(connection) {
  # Create minimal CDM tables needed for testing
  
  # Person table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE person (
      person_id INTEGER PRIMARY KEY,
      gender_concept_id INTEGER,
      year_of_birth INTEGER,
      month_of_birth INTEGER,
      day_of_birth INTEGER,
      birth_datetime TEXT,
      race_concept_id INTEGER,
      ethnicity_concept_id INTEGER
    );
  ")
  
  # Insert sample persons
  DatabaseConnector::executeSql(connection, "
    INSERT INTO person VALUES 
      (1, 8507, 1950, 1, 1, '1950-01-01', 8527, 0),
      (2, 8532, 1960, 6, 15, '1960-06-15', 8527, 0),
      (3, 8507, 1970, 12, 31, '1970-12-31', 8527, 0);
  ")
  
  # Observation period table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE observation_period (
      observation_period_id INTEGER PRIMARY KEY,
      person_id INTEGER,
      observation_period_start_date TEXT,
      observation_period_end_date TEXT,
      period_type_concept_id INTEGER
    );
  ")
  
  DatabaseConnector::executeSql(connection, "
    INSERT INTO observation_period VALUES
      (1, 1, '2000-01-01', '2020-12-31', 0),
      (2, 2, '2005-01-01', '2020-12-31', 0),
      (3, 3, '2010-01-01', '2020-12-31', 0);
  ")
  
  # Vocabulary table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE vocabulary (
      vocabulary_id TEXT PRIMARY KEY,
      vocabulary_name TEXT,
      vocabulary_reference TEXT,
      vocabulary_version TEXT,
      vocabulary_concept_id INTEGER
    );
  ")
  
  DatabaseConnector::executeSql(connection, "
    INSERT INTO vocabulary VALUES
      ('None', 'OMOP Standardized Vocabularies', 'OMOP generated', 'v5.0', 44819096);
  ")
  
  # CDM Source table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE cdm_source (
      cdm_source_name TEXT,
      cdm_source_abbreviation TEXT,
      cdm_holder TEXT,
      source_description TEXT,
      source_documentation_reference TEXT,
      cdm_etl_reference TEXT,
      source_release_date TEXT,
      cdm_release_date TEXT,
      cdm_version TEXT,
      vocabulary_version TEXT
    );
  ")
  
  DatabaseConnector::executeSql(connection, "
    INSERT INTO cdm_source VALUES
      ('Test CDM', 'TEST', 'Test', 'Test database', '', '', '2020-01-01', '2020-01-01', '5.4', 'v5.0');
  ")
  
  # Concept table (minimal)
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE concept (
      concept_id INTEGER PRIMARY KEY,
      concept_name TEXT,
      domain_id TEXT,
      vocabulary_id TEXT,
      concept_class_id TEXT,
      standard_concept TEXT,
      concept_code TEXT,
      valid_start_date TEXT,
      valid_end_date TEXT,
      invalid_reason TEXT
    );
  ")
  
  DatabaseConnector::executeSql(connection, "
    INSERT INTO concept VALUES
      (313217, 'Atrial fibrillation', 'Condition', 'SNOMED', 'Clinical Finding', 'S', '49436004', '1970-01-01', '2099-12-31', NULL),
      (8507, 'MALE', 'Gender', 'Gender', 'Gender', 'S', 'M', '1970-01-01', '2099-12-31', NULL),
      (8532, 'FEMALE', 'Gender', 'Gender', 'Gender', 'S', 'F', '1970-01-01', '2099-12-31', NULL);
  ")
  
  invisible(NULL)
}

#' Create and populate a mock cohort table
#'
#' @param connection Database connection
#' @param schema Schema name (default: "main")
#' @param tableName Table name (default: "cohort")
#' @param cohortData Optional data frame with cohort data. If NULL, creates sample data.
#' @return NULL (modifies database in place)
createMockCohortTable <- function(connection, 
                                  schema = "main", 
                                  tableName = "cohort",
                                  cohortData = NULL) {
  # Create cohort table
  sql <- sprintf("
    CREATE TABLE %s.%s (
      cohort_definition_id INTEGER,
      subject_id INTEGER,
      cohort_start_date TEXT,
      cohort_end_date TEXT
    );
  ", schema, tableName)
  
  DatabaseConnector::executeSql(connection, sql)
  
  # Insert data
  if (is.null(cohortData)) {
    # Create sample cohort data
    sql <- sprintf("
      INSERT INTO %s.%s VALUES
        (1, 1, '2015-01-15', '2015-05-15'),
        (1, 2, '2015-03-20', '2015-08-20'),
        (2, 1, '2016-06-10', '2016-12-10'),
        (2, 3, '2016-09-05', '2017-01-05');
    ", schema, tableName)
    
    DatabaseConnector::executeSql(connection, sql)
  } else {
    # Insert provided data
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = schema,
      tableName = tableName,
      data = cohortData,
      dropTableIfExists = FALSE,
      createTable = FALSE,
      camelCaseToSnakeCase = TRUE
    )
  }
  
  invisible(NULL)
}

#' Create cohort tables using CohortGenerator structure
#'
#' @param connection Database connection
#' @param schema Schema name
#' @param cohortTable Base cohort table name
#' @return Cohort table names list
createCohortTables <- function(connection, schema = "main", cohortTable = "cohort") {
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  
  # Create main cohort table
  createMockCohortTable(connection, schema, cohortTableNames$cohortTable)
  
  # Create inclusion tables
  DatabaseConnector::executeSql(connection, sprintf("
    CREATE TABLE %s.%s (
      cohort_definition_id INTEGER,
      rule_sequence INTEGER,
      name TEXT,
      description TEXT
    );
  ", schema, cohortTableNames$cohortInclusionTable))
  
  DatabaseConnector::executeSql(connection, sprintf("
    CREATE TABLE %s.%s (
      cohort_definition_id INTEGER,
      rule_sequence INTEGER,
      person_count INTEGER,
      gain_count INTEGER,
      person_total INTEGER
    );
  ", schema, cohortTableNames$cohortInclusionResultTable))
  
  DatabaseConnector::executeSql(connection, sprintf("
    CREATE TABLE %s.%s (
      cohort_definition_id INTEGER,
      rule_sequence INTEGER,
      mode_id INTEGER,
      person_count INTEGER,
      gain_count INTEGER,
      person_total INTEGER
    );
  ", schema, cohortTableNames$cohortInclusionStatsTable))
  
  DatabaseConnector::executeSql(connection, sprintf("
    CREATE TABLE %s.%s (
      cohort_definition_id INTEGER,
      base_count INTEGER,
      final_count INTEGER
    );
  ", schema, cohortTableNames$cohortSummaryStatsTable))
  
  DatabaseConnector::executeSql(connection, sprintf("
    CREATE TABLE %s.%s (
      cohort_definition_id INTEGER,
      lost_count INTEGER
    );
  ", schema, cohortTableNames$cohortCensorStatsTable))
  
  return(cohortTableNames)
}

#' Setup a complete test database with CDM and cohort tables
#'
#' @return A list with connection and table information
setupTestDatabase <- function() {
  connection <- createInMemorySqliteDb()
  populateMinimalCdmSchema(connection)
  cohortTableNames <- createCohortTables(connection, schema = "main", cohortTable = "cohort")
  
  list(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
}
