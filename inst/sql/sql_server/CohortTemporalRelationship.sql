IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;

IF OBJECT_ID('tempdb..#target_cohort', 'U') IS NOT NULL
	DROP TABLE #target_cohort;

--- Assign row_id_cs for each unique subject_id and cohort_start_date combination
--HINT DISTRIBUTE_ON_KEY(row_id_cs)
WITH cohort_data
AS (
	SELECT ROW_NUMBER() OVER (
			ORDER BY subject_id ASC,
				cohort_start_date ASC
			) row_id_cs,
		cohort_definition_id,
		subject_id,
		cohort_start_date,
		cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (
			@target_cohort_ids,
			@comparator_cohort_ids
			)
	),
cohort_first_occurrence
AS (
	SELECT cohort_definition_id,
		subject_id,
		MIN(cohort_start_date) cohort_start_date,
		MIN(cohort_end_date) cohort_end_date
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

--HINT DISTRIBUTE_ON_KEY(subject_id)	
SELECT c.*,
	tp.*,
	DATEADD(day, tp.start_day, cohort_start_date) period_begin,
	DATEADD(day, tp.start_day, cohort_start_date) period_end
INTO #target_cohort
FROM #cohort_row_id c,
	#time_periods tp
WHERE cohort_definition_id IN (@target_cohort_ids);

IF OBJECT_ID('tempdb..#cohort_rel_long', 'U') IS NOT NULL
	DROP TABLE #cohort_rel_long;

CREATE TABLE #cohort_rel_long (
	cohort_id BIGINT,
	comparator_cohort_id BIGINT,
	time_id INT,
	relationship_type VARCHAR,
	subjects FLOAT,
	records FLOAT,
	person_days FLOAT,
	records_incidence FLOAT,
	subjects_incidence FLOAT,
	era_incidence FLOAT,
	records_terminate FLOAT,
	subjects_terminate FLOAT
	);

-- cohort time series T1: subjects present in target and comparator cohorts who have atleast one cohort day in time period
--- (i.e. comparator cohort start or comparator cohort end is between (inclusive) time period, or 
--- (comparator cohort start is on/before time period start AND comparator cohort end is on/after time period end))
SELECT t.cohort_definition_id cohort_id,
	c.cohort_definition_id comparator_cohort_id,
	time_id,
	'T1' relationship_type, -- cohort time series by calendar period
	COUNT_BIG(*) records, -- records in calendar month
	COUNT_BIG(DISTINCT subject_id) subjects, -- unique subjects
	SUM(datediff(dd, CASE 
				WHEN c.cohort_start_date >= period_begin
					THEN cohort_start_date
				ELSE period_begin
				END, CASE 
				WHEN c.cohort_end_date >= period_end
					THEN period_end
				ELSE cohort_end_date
				END) + 1) person_days, -- person days within period
	COUNT_BIG(CASE 
			WHEN c.cohort_start_date >= period_begin
				AND c.cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_incidence, -- records incidence within period
	COUNT_BIG(DISTINCT CASE 
			WHEN first_occurrence = 1
				AND c.cohort_start_date >= period_begin
				AND c.cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_incidence, -- subjects incidence within period
	COUNT_BIG(DISTINCT CASE 
			WHEN first_occurrence = 1
				THEN subject_id
			ELSE NULL
			END) era_incidence, -- subjects incidence within period
	COUNT_BIG(CASE 
			WHEN c.cohort_end_date >= period_begin
				AND c.cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) records_terminate, -- records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= period_begin
				AND c.cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_terminate -- subjects terminate within period
INTO #cohort_rel_long
FROM #target_cohort t
INNER JOIN #cohort_row_id c
	ON c.subject_id = t.subject_id
		AND (
			c.cohort_start_date >= period_begin
			AND c.cohort_start_date <= period_end
			) -- cohort starts within calendar period, OR
		OR (
			c.cohort_end_date >= period_begin
			AND c.cohort_end_date <= period_end
			) -- cohort ends within calendar period, OR
		OR (
			c.cohort_end_date >= period_end
			AND c.cohort_start_date <= period_begin
			) -- cohort periods overlaps the calendar period
WHERE c.cohort_definition_id IN (@comparator_cohort_ids)
	AND c.cohort_definition_id != t.cohort_definition_id
GROUP BY t.cohort_definition_id,
	c.cohort_definition_id,
	t.time_id;

IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;

IF OBJECT_ID('tempdb..#target_cohort', 'U') IS NOT NULL
	DROP TABLE #target_cohort;