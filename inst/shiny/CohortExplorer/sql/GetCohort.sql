SELECT subject_id,
	cohort_start_date,
	cohort_end_date,
	YEAR(cohort_start_date) - year_of_birth AS age,
	concept_name AS gender
FROM @cohort_database_schema.@cohort_table
INNER JOIN @cdm_database_schema.person ON person_id = subject_id
INNER JOIN @cdm_database_schema.concept ON gender_concept_id = concept_id
WHERE cohort_definition_id = @cohort_definition_id
	AND subject_id IN (@subject_ids);
