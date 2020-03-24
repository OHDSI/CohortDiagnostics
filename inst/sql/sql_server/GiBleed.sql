DELETE FROM @target_database_schema.@target_cohort_table 
WHERE cohort_definition_id = @target_cohort_id;

INSERT INTO @target_database_schema.@target_cohort_table (
	cohort_definition_id, 
	subject_id, 
	cohort_start_date, 
	cohort_end_date
	)
SELECT CAST(@target_cohort_id AS INT) AS cohort_definition_id,
	condition_occurrence.person_id,
	condition_start_date,
	condition_end_date
FROM @cdm_database_schema.condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM @cdm_database_schema.concept_ancestor
		WHERE ancestor_concept_id = 192671 -- Gastrointestinal haemorrhage
		);
