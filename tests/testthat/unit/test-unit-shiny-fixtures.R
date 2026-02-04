# Shiny Module Test Fixtures

#' Mock Shiny data source
#' @param enabledReports vector of reports to enable
#' @return list structured like a CdDataSource
createMockDataSource <- function(enabledReports = c("all")) {
    dbTable <- data.frame(
        databaseId = "test",
        databaseName = "Test Database",
        databaseIdWithVocabularyVersion = "test (v1)"
    )

    dataSource <- list(
        enabledReports = enabledReports,
        hasData = function(table) TRUE,
        queryData = function(sql, ...) {
            params <- list(...)
            if (grepl("metadata", sql, ignore.case = TRUE) || (any(grepl("metadata", as.character(params))))) {
                return(data.frame(
                    databaseId = "test",
                    startTime = "2023-01-01 00:00:00",
                    variableField = c("timeZone", "runTime", "cdmVersion"),
                    valueField = c("UTC", "10", "5.4")
                ))
            }
            if (grepl("concept_sets", sql, ignore.case = TRUE)) {
                return(data.frame(
                    cohortId = 1,
                    conceptSetId = 1,
                    conceptSetName = "Test Concept Set"
                ))
            }
            data.frame()
        },
        connectionHandler = createMockConnectionHandler(),
        schema = "main",
        cdTablePrefix = "",
        prefixTable = function(t) t,
        dbTable = dbTable,
        databaseTable = dbTable,
        cohortTable = data.frame(cohortId = 1, cohortName = "Test Cohort", shortName = "C1", compoundName = "C1: Test Cohort"),
        conceptSets = data.frame(cohortId = 1, conceptSetId = 1, conceptSetName = "Test Concept Set"),
        temporalAnalysisRef = data.frame(analysisId = 1, analysisName = "Analysis 1", domainId = "Condition"),
        domainIdOptions = c("Condition", "Drug"),
        characterizationTimeIdChoices = data.frame(timeId = 1, temporalChoices = "0d-0d"),
        temporalChoices = data.frame(timeId = 1, temporalChoices = "0d-0d"),
        resultsTemporalTimeRef = data.frame(timeId = 1, temporalChoices = "0d-0d", primaryTimeId = 1, isTemporal = 1, sequence = 1)
    )
    class(dataSource) <- "CdDataSource"
    return(dataSource)
}

#' Mock connection handler
#' @return structure mimicking PooledConnectionHandler
createMockConnectionHandler <- function() {
    handler <- new.env()
    handler$dbms <- function() "sqlite"
    handler$queryDb <- function(sql, ...) {
        params <- list(...)
        if (grepl("temporal_covariate_value", sql, ignore.case = TRUE) || (!is.null(params$table_name) && grepl("temporal_covariate_value", params$table_name, ignore.case = TRUE))) {
            stop(paste("DEBUG STOP: table_name found:", params$table_name))
        }
        if (grepl("metadata|database", sql, ignore.case = TRUE) || (any(grepl("metadata|database", as.character(params))))) {
            return(data.frame(
                databaseId = "test",
                databaseName = "Test Database",
                startTime = "2023-01-01 00:00:00",
                variableField = c("timeZone", "runTime", "cdmVersion"),
                valueField = c("UTC", "10", "5.4")
            ))
        }
        checkCond <- (!is.null(params$table_name) && grepl("temporal_covariate_value", params$table_name, ignore.case = TRUE)) ||
            any(grepl("temporal_covariate_value", as.character(params), ignore.case = TRUE))

        if (grepl("temporal_covariate_value", sql, ignore.case = TRUE) || checkCond) {
            # Check if it's the cohort overlap query (looks for sum_value as both_subjects)
            if (grepl("sum_value as both_subjects", sql, ignore.case = TRUE)) {
                return(data.frame(
                    cohortId = 1,
                    covariateId = 2173, # 2 is comparator + 173 analysis suffix
                    bothSubjects = 10,
                    tOnlySubjects = 90,
                    tFractionInC = 0.1,
                    targetSubjects = 100,
                    databaseId = "test",
                    timeId = 1
                ))
            }
            return(data.frame(
                cohortId = 1,
                timeId = 1,
                databaseId = "test",
                analysisId = 1,
                covariateId = 1001,
                covariateName = "Condition A",
                mean = 0.5,
                sd = 0.1
            ))
        }

        if (grepl("cohort_count", sql, ignore.case = TRUE) ||
            (!is.null(params$cohort_count) && grepl("cohort_count", params$cohort_count, ignore.case = TRUE)) ||
            any(grepl("cohort_count", as.character(params), ignore.case = TRUE))) {
            return(data.frame(
                cohortId = 1,
                cohortEntries = 100,
                cohortSubjects = 100,
                databaseId = "test"
            ))
        }
        data.frame()
    }

    # Use a real in-memory connection to satisfy DBI checks
    conn <- DatabaseConnector::connect(dbms = "sqlite", server = ":memory:")

    handler$getConnection <- function() conn
    handler$closeConnection <- function() DatabaseConnector::disconnect(conn)

    structure(
        handler,
        class = "PooledConnectionHandler"
    )
}

#' Mock result database settings
#' @return list of settings
createMockResultDatabaseSettings <- function() {
    list(
        schema = "main",
        vocabularyDatabaseSchema = "main",
        cdTablePrefix = "",
        cgTable = "cohort",
        databaseTable = "database",
        databaseTablePrefix = "",
        connectionDetails = list(dbms = "sqlite", server = ":memory:")
    )
}

getResultsTemporalTimeRef <- function(dataSource) {
    dataSource$resultsTemporalTimeRef
}

resolvedConceptSet <- function(...) data.frame(conceptId = 1, databaseId = "test", cohortId = 1, conceptSetId = 1)
mappedConceptSet <- function(...) data.frame(conceptId = 1, databaseId = "test", cohortId = 1, conceptSetId = 1)
