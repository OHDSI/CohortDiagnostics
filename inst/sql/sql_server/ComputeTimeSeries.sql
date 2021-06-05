-- #time_series
IF OBJECT_ID('tempdb..#time_series', 'U') IS NOT NULL
	DROP TABLE #time_series;
	
IF OBJECT_ID('tempdb..#time_series1', 'U') IS NOT NULL
	DROP TABLE #time_series1;

IF OBJECT_ID('tempdb..#time_series2', 'U') IS NOT NULL
	DROP TABLE #time_series2;

IF OBJECT_ID('tempdb..#time_series3', 'U') IS NOT NULL
	DROP TABLE #time_series3;

IF OBJECT_ID('tempdb..#time_series4', 'U') IS NOT NULL
	DROP TABLE #time_series4;

IF OBJECT_ID('tempdb..#time_series5', 'U') IS NOT NULL
	DROP TABLE #time_series5;

IF OBJECT_ID('tempdb..#time_series6', 'U') IS NOT NULL
	DROP TABLE #time_series6;

SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'ci' series_type,
	COUNT_BIG(*) records,
	COUNT_BIG(DISTINCT subject_id) subjects,
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
			END) subjects_incidence
INTO #time_series1
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
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'oi' series_type,
	COUNT_BIG(*) records,
	COUNT_BIG(DISTINCT person_id) subjects,
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
			END) subjects_incidence
INTO #time_series2
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	SELECT DISTINCT cohort_definition_id,
		subject_id
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	) c ON o.person_id = c.subject_id
INNER JOIN #calendar_periods cp ON (
		observation_period_start_date >= period_begin
		AND observation_period_start_date <= period_end
		)
	OR (
		observation_period_end_date >= period_begin
		AND observation_period_end_date <= period_end
		)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

SELECT 0 cohort_id,
	period_begin,
	calendar_interval,
	'di' series_type,
	COUNT_BIG(*) records,
	COUNT_BIG(DISTINCT person_id) subjects,
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
			END) subjects_incidence
INTO #time_series3
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp ON (
		observation_period_start_date >= period_begin
		AND observation_period_start_date <= period_end
		)
	OR (
		observation_period_end_date >= period_begin
		AND observation_period_end_date <= period_end
		)
GROUP BY period_begin,
	calendar_interval;

SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'cp' series_type,
	COUNT_BIG(*) records,
	COUNT_BIG(DISTINCT subject_id) subjects,
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
			END) subjects_incidence
INTO #time_series4
FROM @cohort_database_schema.@cohort_table
INNER JOIN #calendar_periods cp ON cohort_start_date <= period_end
	AND cohort_end_date >= period_begin
WHERE cohort_definition_id IN (@cohort_ids)
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

SELECT cohort_definition_id cohort_id,
	period_begin,
	calendar_interval,
	'op' series_type,
	COUNT_BIG(*) records,
	COUNT_BIG(DISTINCT person_id) subjects,
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
			END) subjects_incidence
INTO #time_series5
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	SELECT DISTINCT cohort_definition_id,
		subject_id
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@cohort_ids)
	) c ON o.person_id = c.subject_id
INNER JOIN #calendar_periods cp ON observation_period_start_date <= period_end
	AND observation_period_end_date >= period_begin
GROUP BY period_begin,
	calendar_interval,
	cohort_definition_id;

SELECT 0 cohort_id,
	period_begin,
	calendar_interval,
	'dp' series_type,
	COUNT_BIG(*) records,
	COUNT_BIG(DISTINCT person_id) subjects,
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
			END) subjects_incidence
INTO #time_series6
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp ON observation_period_start_date <= period_end
	AND observation_period_end_date >= period_begin
GROUP BY period_begin,
	calendar_interval;

SELECT *
INTO #time_series
FROM (
	SELECT *
	FROM #time_series1
	
	UNION
	
	SELECT *
	FROM #time_series2
	
	UNION
	
	SELECT *
	FROM #time_series3
	
	UNION
	
	SELECT *
	FROM #time_series4
	
	UNION
	
	SELECT *
	FROM #time_series5
	
	UNION
	
	SELECT *
	FROM #time_series6
	) f;
	
	

	
IF OBJECT_ID('tempdb..#time_series1', 'U') IS NOT NULL
	DROP TABLE #time_series1;

IF OBJECT_ID('tempdb..#time_series2', 'U') IS NOT NULL
	DROP TABLE #time_series2;

IF OBJECT_ID('tempdb..#time_series3', 'U') IS NOT NULL
	DROP TABLE #time_series3;

IF OBJECT_ID('tempdb..#time_series4', 'U') IS NOT NULL
	DROP TABLE #time_series4;

IF OBJECT_ID('tempdb..#time_series5', 'U') IS NOT NULL
	DROP TABLE #time_series5;

IF OBJECT_ID('tempdb..#time_series6', 'U') IS NOT NULL
	DROP TABLE #time_series6;