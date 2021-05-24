SELECT cohort_definition_id cohort_id
  ,period_begin
	,calendar_interval
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
WHERE cohort_definition_id IN (@cohort_ids)
GROUP BY period_begin
	,calendar_interval
	,cohort_definition_id;
