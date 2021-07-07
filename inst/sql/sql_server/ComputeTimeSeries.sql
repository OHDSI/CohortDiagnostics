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

/*	
IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;
	
--- Assign row_id_cs for each unique subject_id and cohort_start_date combination
--HINT DISTRIBUTE_ON_KEY(subject_id)
WITH cohort_data
AS (
	SELECT ROW_NUMBER() OVER (
			PARTITION BY subject_id, cohort_start_date
			) row_id_cs,
		cohort_definition_id,
		subject_id,
		cohort_start_date,
		cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (
			@cohort_ids
			)
	),
cohort_first_occurrence
AS (
	SELECT cohort_definition_id,
		subject_id,
		MIN(cohort_start_date) cohort_start_date
	FROM cohort_data
	GROUP BY cohort_definition_id,
		subject_id
	)
SELECT cd.row_id_cs,
	cd.cohort_definition_id,
	cd.subject_id,
	cd.cohort_start_date,
	cd.cohort_end_date,
	CASE 
		WHEN cd.cohort_start_date = fo.cohort_start_date
			THEN 1
		ELSE 0
		END first_occurrence
INTO #cohort_row_id
FROM cohort_data cd
INNER JOIN cohort_first_occurrence fo
	ON fo.cohort_definition_id = cd.cohort_definition_id
		AND fo.subject_id = cd.subject_id;
*/
-- cohort time series T1: subjects in the cohort who have atleast one cohort day in calendar period
--- (i.e. cohort start or cohort end is between (inclusive) calendar period, or 
--- (cohort start is on/before calendar start AND cohort end is on/after calendar end))
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
			END) records_start, -- records start within period
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_start, -- subjects start within period
	COUNT_BIG(CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_end, -- records end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_end -- subjects end within period
INTO #c_time_series1
FROM @cohort_database_schema.@cohort_table
INNER JOIN #calendar_periods cp
	ON (
		cohort_start_date >= period_begin
		AND cohort_start_date <= period_end
		) -- cohort starts within calendar period, OR
	OR (
		cohort_end_date >= period_begin
		AND cohort_end_date <= period_end
		) -- cohort ends within calendar period, OR
	OR (
		cohort_end_date >= period_end
		AND cohort_start_date <= period_begin
		) -- cohort periods overlaps the calendar period
WHERE cohort_definition_id IN (@cohort_ids)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;
/*
-- cohort time series T2: subjects in the cohort who have atleast one observation day in calendar period
--- (i.e. observation start or observation end is between (inclusive) calendar period, or 
--- (observation start is on/before calendar start AND observation end is on/after calendar end))
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
INTO #c_time_series2
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	-- limiting to the cohort
	SELECT DISTINCT cohort_definition_id,
		subject_id
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	) c
	ON o.person_id = c.subject_id
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
	calendar_interval,
	cohort_definition_id;

-- database time series T3: persons in the data source who have atleast one observation day in calendar period
--- (i.e. observation start or observation end is between (inclusive) calendar period, or 
--- (observation start is on/before calendar start AND observation end is on/after calendar end))
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
INTO #d_time_series3
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

-- cohort time series T4: subjects in the cohorts whose cohort period are embedded within calendar period
--- (cohort start is between (inclusive) calendar period, AND 
--- (cohort end is between (inclusive) calendar period)
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
			END) records_start,
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_start_date >= period_begin
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
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_end -- subjects end within period
INTO #c_time_series4
FROM @cohort_database_schema.@cohort_table
INNER JOIN #calendar_periods cp
	ON (
		cohort_start_date <= period_end -- calendar period start on or before calendar period end, AND
		AND cohort_end_date >= period_begin -- calendar period end on or after calendar period begins
		)
WHERE cohort_definition_id IN (@cohort_ids)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

-- cohort time series T5: subjects in the cohorts whose observation period is embedded within calendar period
--- (cohort start is between (inclusive) calendar period, AND 
--- (cohort end is between (inclusive) calendar period)
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
INTO #c_time_series5
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	SELECT DISTINCT cohort_definition_id,
		subject_id
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	) c
	ON o.person_id = c.subject_id
INNER JOIN #calendar_periods cp
	ON (
		observation_period_start_date <= period_end -- observation period start on or before calendar period end, AND
		AND observation_period_end_date >= period_begin -- observation period end on or before calendar period end
		)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;


-- datasource time series T5: persons in the observation table whose observation period is embedded within calendar period
--- (observation start is between (inclusive) calendar period, AND 
--- (observation end is between (inclusive) calendar period)
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
INTO #d_time_series6
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp ON (
		observation_period_start_date <= period_end
		AND observation_period_end_date >= period_begin
		)
GROUP BY period_begin,
	calendar_interval;
*/
-- union all data
SELECT *
INTO #time_series
FROM (
	SELECT *
	FROM #c_time_series1
		/*	
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
	*/
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

IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;