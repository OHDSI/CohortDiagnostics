/*
IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;
*/
IF OBJECT_ID('tempdb..#cohort_rel', 'U') IS NOT NULL
	DROP TABLE #cohort_rel;

/*
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
			@target_cohort_ids,
			@comparator_cohort_ids
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
-- subjects present in target and comparator cohorts who have atleast one cohort day in time period
--- (i.e. comparator cohort start or comparator cohort end is between (inclusive) time period, or 
--- (comparator cohort start is on/before time period start AND comparator cohort end is on/after time period end))
SELECT t.cohort_definition_id cohort_id,
	c.cohort_definition_id comparator_cohort_id,
	tp.time_id,
	COUNT_BIG(*) records, -- comparator cohort records in time period (includes overlap)
	COUNT_BIG(DISTINCT c.subject_id) subjects, -- comparator cohort subjects in time period (includes overlap)
	SUM(datediff(dd, CASE 
				WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
					THEN c.cohort_start_date
				ELSE DATEADD(day, tp.start_day, t.cohort_start_date)
				END, CASE 
				WHEN c.cohort_end_date >= DATEADD(day, tp.end_day, t.cohort_start_date)
					THEN DATEADD(day, tp.end_day, t.cohort_start_date)
				ELSE c.cohort_end_date
				END) + 1) person_days, -- comparator cohort person days within period
	COUNT_BIG(CASE 
			WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) records_start, -- comparator cohorts records incidence within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) subjects_start, -- comparator cohort subjects incidence within period (true incidence)
	COUNT_BIG(CASE 
			WHEN c.cohort_end_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) records_end, -- comparator cohort records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) subjects_end -- comparator cohort subjects terminate within period
INTO #cohort_rel
FROM #time_periods tp -- offset
CROSS JOIN @cohort_database_schema.@cohort_table t
INNER JOIN @cohort_database_schema.@cohort_table c
	ON c.subject_id = t.subject_id
		AND c.cohort_definition_id != t.cohort_definition_id
		AND (
			-- comparator cohort dates are computed in relation to target cohort start date + offset
			-- Offset: is the time period
			(
				c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				) -- comparator cohort starts within period, OR
			OR (
				c.cohort_end_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				) -- comparator cohort ends within period, OR
			OR (
				c.cohort_end_date >= DATEADD(day, tp.end_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.start_day, t.cohort_start_date)
				) -- comparator cohort periods overlaps the period
			)
WHERE c.cohort_definition_id IN (@comparator_cohort_ids)
	AND t.cohort_definition_id IN (@target_cohort_ids)
GROUP BY t.cohort_definition_id,
	c.cohort_definition_id,
	tp.time_id;
	/*
IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;
	*/
