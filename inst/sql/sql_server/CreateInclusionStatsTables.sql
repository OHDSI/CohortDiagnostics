IF OBJECT_ID('@cohort_database_schema.@cohort_inclusion_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@cohort_inclusion_table;
	
IF OBJECT_ID('@cohort_database_schema.@cohort_inclusion_result_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@cohort_inclusion_result_table;
	
IF OBJECT_ID('@cohort_database_schema.@cohort_inclusion_stats_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@cohort_inclusion_stats_table;
	
IF OBJECT_ID('@cohort_database_schema.@cohort_summary_stats_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@cohort_summary_stats_table;

CREATE TABLE @cohort_database_schema.@cohort_inclusion_table (
	cohort_definition_id INT NOT NULL,
	rule_sequence INT NOT NULL,
	name VARCHAR(255) NULL,
	description VARCHAR(1000) NULL
	);

CREATE TABLE @cohort_database_schema.@cohort_inclusion_result_table (
	cohort_definition_id INT NOT NULL,
	inclusion_rule_mask BIGINT NOT NULL,
	person_count BIGINT NOT NULL,
	mode_id INT
	);

CREATE TABLE @cohort_database_schema.@cohort_inclusion_stats_table (
	cohort_definition_id INT NOT NULL,
	rule_sequence INT NOT NULL,
	person_count BIGINT NOT NULL,
	gain_count BIGINT NOT NULL,
	person_total BIGINT NOT NULL,
	mode_id INT
	);

CREATE TABLE @cohort_database_schema.@cohort_summary_stats_table (
	cohort_definition_id INT NOT NULL,
	base_count BIGINT NOT NULL,
	final_count BIGINT NOT NULL,
	mode_id INT
	);
