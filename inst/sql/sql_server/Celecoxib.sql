DELETE FROM @target_database_schema.@target_cohort_table 
WHERE cohort_definition_id = @target_cohort_id;

INSERT INTO @target_database_schema.@target_cohort_table (
	cohort_definition_id, 
	subject_id, 
	cohort_start_date, 
	cohort_end_date
	)
SELECT CAST(@target_cohort_id AS INT) AS cohort_definition_id,
	person_id,
	drug_era_start_date,
	drug_era_end_date
FROM @cdm_database_schema.drug_era
WHERE drug_concept_id = 1118084;-- celecoxib
