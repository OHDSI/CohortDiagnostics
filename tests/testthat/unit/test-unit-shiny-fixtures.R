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
        queryData = function(sql, ...) data.frame(),
        connectionHandler = createMockConnectionHandler(),
        schema = "main",
        cdTablePrefix = "",
        prefixTable = function(t) t,
        dbTable = dbTable,
        databaseTable = dbTable,
        cohortTable = data.frame(cohortId = 1, cohortName = "Test Cohort", shortName = "C1", compoundName = "C1: Test Cohort")
    )
    class(dataSource) <- "CdDataSource"
    return(dataSource)
}

#' Mock connection handler
#' @return structure mimicking PooledConnectionHandler
createMockConnectionHandler <- function() {
    handler <- new.env()
    handler$dbms <- function() "sqlite"
    handler$queryDb <- function(sql, ...) data.frame()

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
