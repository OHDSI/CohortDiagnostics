-- cohort time series T5: subjects in the cohorts whose observation period is embedded within calendar period
--- (cohort start is between (inclusive) calendar period, AND 
--- (cohort end is between (inclusive) calendar period)
SELECT cohort_definition_id cohort_id,
	time_id,
	COUNT_BIG(*) records, -- records in calendar month
	COUNT_BIG(DISTINCT subject_id) subjects, -- unique subjects
	SUM(datediff(dd, CASE 
				WHEN observation_period_start_date >= period_begin
					THEN observation_period_start_date
				ELSE period_begin
				END, CASE 
				WHEN observation_period_end_date >= period_end
					THEN period_end
				ELSE observation_period_end_date
				END) + 1) person_days,
	COUNT_BIG(CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_start,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_start,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_end, -- records end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_end -- subjects end within period
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	SELECT DISTINCT cohort_definition_id,
		subject_id
	FROM #cohort_ts
	) c ON o.person_id = c.subject_id
INNER JOIN #calendar_periods cp ON (
		observation_period_start_date <= period_end -- observation period start on or before calendar period end, AND
		AND observation_period_end_date >= period_begin -- observation period end on or before calendar period end
		)
GROUP BY time_id,
	cohort_definition_id;