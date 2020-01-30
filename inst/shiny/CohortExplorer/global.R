# oracleTempSchema <- NULL
# cdmDatabaseSchema <- "cdm_truven_mdcd_v610.dbo"
# cohortDatabaseSchema <- "scratch.dbo"
# cohortTable <- "coxibVsNonselVsGiBleed"
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
#                                                                 server = Sys.getenv("PDW_SERVER"),
#                                                                 user = NULL,
#                                                                 password = NULL,
#                                                                 port = Sys.getenv("PDW_PORT"))
# 
# cohortDefinitionId = 1
# shinySettings <- list(connectionDetails = connectionDetails,
#                       cdmDatabaseSchema = cdmDatabaseSchema,
#                       cohortDatabaseSchema = cohortDatabaseSchema,
#                       cohortTable = cohortTable,
#                       cohortDefinitionId = cohortDefinitionId,
#                       sampleSize = 10)

conn <- DatabaseConnector::connect(shinySettings$connectionDetails)

if (is.null(shinySettings$subjectIds)) {
  sql <- "SELECT TOP @sample_size subject_id
  FROM (
    SELECT DISTINCT subject_id 
    FROM @cohort_database_schema.@cohort_table 
    WHERE cohort_definition_id = @cohort_definition_id
  ) all_ids
  ORDER BY NEWID();"
  sql <- SqlRender::render(sql = sql, 
                           sample_size = shinySettings$sampleSize,
                           cohort_database_schema = shinySettings$cohortDatabaseSchema,
                           cohort_table = shinySettings$cohortTable,
                           cohort_definition_id = shinySettings$cohortDefinitionId)
  sql <- SqlRender::translate(sql = sql, targetDialect = shinySettings$connectionDetails$dbms)
  subjectIds <- DatabaseConnector::querySql(conn, sql)[, 1]
} else {
  subjectIds <- shinySettings$subjectIds 
}

if (length(subjectIds) == 0) {
  stop("No subjects found in cohort ", shinySettings$cohortDefinitionId)
}

sql <- SqlRender::readSql("GetCohort.sql")
sql <- SqlRender::render(sql = sql, 
                         cohort_database_schema = shinySettings$cohortDatabaseSchema,
                         cohort_table = shinySettings$cohortTable,
                         cohort_definition_id = shinySettings$cohortDefinitionId,
                         cdm_database_schema = shinySettings$cdmDatabaseSchema,
                         subject_ids = subjectIds)
sql <- SqlRender::translate(sql = sql, targetDialect = shinySettings$connectionDetails$dbms)
cohort <- DatabaseConnector::querySql(conn, sql, snakeCaseToCamelCase = TRUE)
cohort <- cohort[order(cohort$subjectId, cohort$cohortStartDate), ]
subjectIds <- unique(cohort$subjectId)

if (nrow(cohort) == 0) {
  stop("Cohort is empty") 
}
eventSql <- SqlRender::readSql("GetEvents.sql")
