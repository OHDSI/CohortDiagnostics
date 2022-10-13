-- datasource time series T6: persons in the observation table whose observation period is embedded within calendar period
--- (observation start is between (inclusive) calendar period, AND 
--- (observation end is between (inclusive) calendar period)
SELECT -44819062 cohort_id,
	time_id,
	COUNT_BIG(DISTINCT CONCAT(cast(person_id AS VARCHAR(30)), '_', cast(observation_period_start_date AS VARCHAR(30)))) records, -- records in calendar month
	COUNT_BIG(DISTINCT person_id) subjects, -- unique subjects
	SUM(datediff(dd, CASE 
				WHEN observation_period_start_date >= period_begin
					THEN observation_period_start_date
				ELSE period_begin
				END, CASE 
				WHEN observation_period_end_date >= period_end
					THEN period_end
				ELSE observation_period_end_date
				END) + 1) person_days,
	0 person_days_in,
	COUNT_BIG(CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN person_id
			ELSE NULL
			END) records_start,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN person_id
			ELSE NULL
			END) subjects_start,
	0 subjects_start_in,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN person_id
			ELSE NULL
			END) records_end, -- records end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN person_id
			ELSE NULL
			END) subjects_end, -- subjects end within period
	0 subjects_end_in
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp ON (
		observation_period_start_date <= period_end
		AND observation_period_end_date >= period_begin
		)
GROUP BY time_id;
