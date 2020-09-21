IF OBJECT_ID('@cohort_database_schema.@unique_concept_id_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@unique_concept_id_table;

CREATE TABLE @cohort_database_schema.@unique_concept_id_table (
	database_id varchar(200),
	cohort_id BIGINT,
	task varchar(200),
	concept_id INT
	);
