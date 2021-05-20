SELECT period_begin
	,period_end
	,cohort_definition_id cohort_id
	,FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10) AS age_group
	,gender_concept_id
	,COUNT_BIG(*) records
	,COUNT_BIG(DISTINCT subject_id) subjects
	,SUM(datediff(dd, CASE 
				WHEN cohort_start_date >= period_begin
					THEN cohort_start_date
				ELSE period_begin
				END, CASE 
				WHEN cohort_end_date >= period_end
					THEN period_end
				ELSE cohort_end_date
				END) + 1) person_days
	,COUNT_BIG(CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_incidence
	,COUNT_BIG(DISTINCT CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_incidence
FROM @cohort_database_schema.@cohort_table
INNER JOIN #calendar_periods cp ON (
		cohort_start_date >= period_begin
		AND cohort_start_date <= period_end
		)
	OR (
		cohort_end_date >= period_begin
		AND cohort_end_date <= period_end
		)
INNER JOIN @cdm_database_schema.person ON subject_id = person.person_id
WHERE cohort_definition_id IN c(@cohort_ids)
GROUP BY period_begin
	,period_end
	,cohort_definition_id
	,FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10) AS age_group
	,gender_concept_id;
