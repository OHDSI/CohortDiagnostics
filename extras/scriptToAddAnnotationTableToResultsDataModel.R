sqliteDbPath <-
  file.path(rstudioapi::getActiveProject(),
            "data/MergedCohortDiagnosticsData.sqlite")
connectionDetails <-
  DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
conn <- DatabaseConnector::connect(connectionDetails)
# Drop IF EXISTS
# sql <- "DROP TABLE main.annotation"
sql <- "--Table annotation
DROP TABLE IF EXISTS @results_schema.@annotation;
CREATE TABLE @results_schema.@annotation (
      annotation_id BIGINT NOT NULL,
			created_by VARCHAR NOT NULL,
			created_on BIGINT NOT NULL,
			modified_last_on BIGINT,
			deleted_on BIGINT,
			annotation VARCHAR NOT NULL,
			PRIMARY KEY(annotation_id)
);

--Table annotation_link

DROP TABLE IF EXISTS @results_schema.@annotation_link;
CREATE TABLE @results_schema.@annotation_link (
      annotation_id BIGINT NOT NULL DEFAULT 0,
			diagnostics_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(annotation_id, diagnostics_id, cohort_id, database_id)
);

--Table annotation_score

DROP TABLE IF EXISTS @results_schema.@annotation_attributes;
CREATE TABLE @results_schema.@annotation_attributes (
      annotation_id BIGINT NOT NULL DEFAULT 0,
			created_by VARCHAR NOT NULL,
			annotation_attributes VARCHAR NOT NULL,
			created_on BIGINT NOT NULL,
			PRIMARY KEY(annotation_id, created_by)
);"

DatabaseConnector::renderTranslateExecuteSql(
  connection = conn,
  sql = sql,
  results_schema = "main",
  annotation = "annotation",
  annotation_link = "annotation_link",
  annotation_attributes = "annotation_attributes"
)
