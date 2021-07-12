-- database time series T3: persons in the data source who have atleast one observation day in calendar period
--- (i.e. observation start or observation end is between (inclusive) calendar period, or 
--- (observation start is on/before calendar start AND observation end is on/after calendar end))
SELECT 0 cohort_id,
	period_begin,
	calendar_interval,
	COUNT_BIG(*) records, -- records in calendar month
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
			END) subjects_end -- subjects end within period
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp
	ON (
		observation_period_start_date >= period_begin
		AND observation_period_start_date <= period_end
		) -- observation period starts within calendar period, OR
	OR (
		observation_period_end_date >= period_begin
		AND observation_period_end_date <= period_end
		) -- observation period end within calendar period, OR
	OR (
		observation_period_end_date >= period_end
		AND observation_period_start_date <= period_begin
		) -- observation period overlaps the calendar period
GROUP BY period_begin,
	calendar_interval;
