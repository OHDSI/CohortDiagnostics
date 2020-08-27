SELECT @domain_concept_id AS concept_id,
	COUNT(*) AS concept_count
FROM @cohort_database_schema.@cohort_table
INNER JOIN @cdm_database_schema.@domain_table
	ON subject_id = person_id
		AND cohort_start_date = @domain_start_date
INNER JOIN #Codesets
	ON @domain_concept_id = concept_id
WHERE cohort_definition_id = @cohort_id
	AND codeset_id IN (@primary_codeset_ids)
GROUP BY @domain_concept_id
;
