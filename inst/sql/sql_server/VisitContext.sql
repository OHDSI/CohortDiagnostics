IF OBJECT_ID('tempdb..@visit_context_table', 'U') IS NOT NULL
	DROP TABLE @visit_context_table;

SELECT c1.cohort_definition_id AS cohort_id,
	vo1.visit_concept_id,
	CASE 
		WHEN vo1.visit_end_date < c1.cohort_start_date
			THEN 'Before'
		WHEN vo1.visit_start_date < c1.cohort_start_date
			AND vo1.visit_end_date >= c1.cohort_start_date
			THEN 'During visit'
		WHEN vo1.visit_start_date = c1.cohort_start_date
			THEN 'On visit start'
		WHEN vo1.visit_start_date > c1.cohort_start_date
			THEN 'After'
		ELSE 'Other'
		END AS visit_context,
	COUNT(DISTINCT vo1.person_id) AS subjects
INTO @visit_context_table
FROM @cohort_database_schema.@cohort_table c1
INNER JOIN @cdm_database_schema.visit_occurrence vo1
	ON c1.subject_id = vo1.person_id
		AND vo1.visit_end_date >= dateadd(dd, - 30, c1.cohort_start_date)
		AND vo1.visit_start_date <= dateadd(dd, 30, c1.cohort_start_date)
WHERE cohort_definition_id IN (@cohort_ids)
GROUP BY c1.cohort_definition_id,
	vo1.visit_concept_id,
	CASE 
		WHEN vo1.visit_end_date < c1.cohort_start_date
			THEN 'Before'
		WHEN vo1.visit_start_date < c1.cohort_start_date
			AND vo1.visit_end_date >= c1.cohort_start_date
			THEN 'During visit'
		WHEN vo1.visit_start_date = c1.cohort_start_date
			THEN 'On visit start'
		WHEN vo1.visit_start_date > c1.cohort_start_date
			THEN 'After'
		ELSE 'Other'
		END;
