-- cohort time series T4: subjects in the cohorts whose cohort period are embedded within calendar period
--- (cohort start is between (inclusive) calendar period, AND 
--- (cohort end is between (inclusive) calendar period)
SELECT cohort_definition_id cohort_id,
	time_id,
	COUNT_BIG(*) records, -- records in calendar month
	COUNT_BIG(DISTINCT subject_id) subjects, -- unique subjects
	SUM(datediff(dd, CASE 
				WHEN cohort_start_date >= period_begin
					THEN cohort_start_date
				ELSE period_begin
				END, CASE 
				WHEN cohort_end_date >= period_end
					THEN period_end
				ELSE cohort_end_date
				END) + 1) person_days,
	COUNT_BIG(CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_start,
	COUNT_BIG(DISTINCT CASE 
			WHEN first_occurrence = 'Y'
				AND cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_start,
	COUNT_BIG(CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_end, -- records end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN first_occurrence = 'Y'
				AND cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_end -- subjects end within period
FROM #cohort_ts
INNER JOIN #calendar_periods cp ON (
		cohort_start_date <= period_end -- calendar period start on or before calendar period end, AND
		AND cohort_end_date >= period_begin -- calendar period end on or after calendar period begins
		)
GROUP BY time_id,
	cohort_definition_id;