-- #time_series
IF OBJECT_ID('tempdb..#time_series', 'U') IS NOT NULL
	DROP TABLE #time_series;

IF OBJECT_ID('tempdb..#c_time_series1', 'U') IS NOT NULL
	DROP TABLE #c_time_series1;

IF OBJECT_ID('tempdb..#c_time_series2', 'U') IS NOT NULL
	DROP TABLE #c_time_series2;

IF OBJECT_ID('tempdb..#d_time_series3', 'U') IS NOT NULL
	DROP TABLE #d_time_series3;

IF OBJECT_ID('tempdb..#c_time_series4', 'U') IS NOT NULL
	DROP TABLE #c_time_series4;

IF OBJECT_ID('tempdb..#c_time_series5', 'U') IS NOT NULL
	DROP TABLE #c_time_series5;

IF OBJECT_ID('tempdb..#d_time_series6', 'U') IS NOT NULL
	DROP TABLE #d_time_series6;

-- cohort time series
-- subjects in the cohort whose cohort period overlaps calendar period
SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'T1' series_type, -- cohort time series by calendar period
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
				END) + 1) person_days, -- person days within period
	COUNT_BIG(CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_incidence, -- records incidence within period
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_incidence, -- subjects incidence within period
	COUNT_BIG(CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_terminate, -- records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_terminate -- subjects terminate within period
INTO #c_time_series1
FROM @cohort_database_schema.@cohort_table
INNER JOIN #calendar_periods cp ON (
		cohort_start_date >= period_begin
		AND cohort_start_date <= period_end
		) -- cohort starts within calendar period, OR
	OR (
		cohort_end_date >= period_begin
		AND cohort_end_date <= period_end
		) -- cohort end within calendar period, OR
	OR (
		cohort_end_date >= period_end
		AND cohort_start_date <= period_begin
		) -- cohort periods overlaps the calendar period
WHERE cohort_definition_id IN (@cohort_ids)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

-- cohort time series
-- subjects in the cohort whose observation period overlaps calendar period
SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'T2' series_type, -- cohort time series by calendar period
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
				END) + 1) person_days, -- person days within period
	COUNT_BIG(CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_incidence,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_incidence,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_terminate, -- records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_terminate -- subjects terminate within period
INTO #c_time_series2
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	-- limiting to the cohort
	SELECT DISTINCT cohort_definition_id,
		subject_id
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	) c ON o.person_id = c.subject_id
INNER JOIN #calendar_periods cp ON (
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
	calendar_interval,
	cohort_definition_id;

-- database time series
-- subjects in the database whose observation period overlaps calendar period
SELECT 0 cohort_id,
	period_begin,
	calendar_interval,
	'T3' series_type, -- observation period time series by calendar period
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
			END) records_incidence,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN person_id
			ELSE NULL
			END) subjects_incidence,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN person_id
			ELSE NULL
			END) records_terminate, -- records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN person_id
			ELSE NULL
			END) subjects_terminate -- subjects terminate within period
INTO #d_time_series3
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp ON (
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

-- cohort time series
-- subjects in cohort, whose cohort period is embeded within the calendar period
SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'T4' series_type, -- cohort time series by calendar period
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
			END) records_incidence,
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_incidence,
	COUNT_BIG(CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_terminate, -- records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_terminate -- subjects terminate within period
INTO #c_time_series4
FROM @cohort_database_schema.@cohort_table
INNER JOIN #calendar_periods cp ON (
		cohort_start_date <= period_end -- calendar period start on or before calendar period end, AND
		AND cohort_end_date >= period_begin -- calendar period end on or after calendar period begins
		)
WHERE cohort_definition_id IN (@cohort_ids)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

-- cohort time series
-- subjects in cohort, whose observation period is embeded within the calendar period
SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'T5' series_type, -- cohort time series by calendar period
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
			END) records_incidence,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_incidence,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_terminate, -- records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_terminate -- subjects terminate within period
INTO #c_time_series5
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	SELECT DISTINCT cohort_definition_id,
		subject_id
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	) c ON o.person_id = c.subject_id
INNER JOIN #calendar_periods cp ON (
		observation_period_start_date <= period_end -- observation period start on or before calendar period end, AND
		AND observation_period_end_date >= period_begin -- observation period end on or before calendar period end
		)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

-- database time series
-- subjects in the database whose observation period is embedded within calendar period
SELECT 0 cohort_id,
	period_begin,
	calendar_interval,
	'T6' series_type, -- cohort time series by calendar period
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
			END) records_incidence,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN person_id
			ELSE NULL
			END) subjects_incidence,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN person_id
			ELSE NULL
			END) records_terminate, -- records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN person_id
			ELSE NULL
			END) subjects_terminate -- subjects terminate within period
INTO #d_time_series6
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp ON (
		observation_period_start_date <= period_end
		AND observation_period_end_date >= period_begin
		)
GROUP BY period_begin,
	calendar_interval;

-- union all data
SELECT *
INTO #time_series
FROM (
	SELECT *
	FROM #c_time_series1
	
	UNION
	
	SELECT *
	FROM #c_time_series2
	
	UNION
	
	SELECT *
	FROM #d_time_series3
	
	UNION
	
	SELECT *
	FROM #c_time_series4
	
	UNION
	
	SELECT *
	FROM #c_time_series5
	
	UNION
	
	SELECT *
	FROM #d_time_series6
	) f;

IF OBJECT_ID('tempdb..#c_time_series1', 'U') IS NOT NULL
	DROP TABLE #c_time_series1;

IF OBJECT_ID('tempdb..#c_time_series2', 'U') IS NOT NULL
	DROP TABLE #c_time_series2;

IF OBJECT_ID('tempdb..#d_time_series3', 'U') IS NOT NULL
	DROP TABLE #d_time_series3;

IF OBJECT_ID('tempdb..#c_time_series4', 'U') IS NOT NULL
	DROP TABLE #c_time_series4;

IF OBJECT_ID('tempdb..#c_time_series5', 'U') IS NOT NULL
	DROP TABLE #c_time_series5;

IF OBJECT_ID('tempdb..#d_time_series6', 'U') IS NOT NULL
	DROP TABLE #d_time_series6;
