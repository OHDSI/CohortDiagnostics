IF OBJECT_ID('tempdb..#cohort_rel', 'U') IS NOT NULL
	DROP TABLE #cohort_rel;


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
-- subjects present in target but not comparator cohorts
SELECT t.cohort_definition_id cohort_id,
	c.cohort_definition_id comparator_cohort_id,
	tp.time_id,
	COUNT_BIG(CASE WHEN c.cohort_definition_id IS NULL THEN t.cohort_definition_id else NULL END) target_records,
	COUNT_BIG(DISTINCT CASE WHEN c.cohort_definition_id IS NULL THEN t.subject_id else NULL END) target_subjects,
	COUNT_BIG(CASE WHEN t.cohort_definition_id IS NULL THEN c.cohort_definition_id else NULL END) comparator_records,
	COUNT_BIG(DISTINCT CASE WHEN t.cohort_definition_id IS NULL THEN c.subject_id else NULL END) comparator_subjects
INTO #cohort_rel
FROM #time_periods tp -- offset
CROSS JOIN (SELECT * FROM @cohort_database_schema.@cohort_table WHERE c.cohort_definition_id IN (@target_cohort_ids)) t
FULL OUTER JOIN (SELECT * FROM @cohort_database_schema.@cohort_table WHERE c.cohort_definition_id IN (@comparator_cohort_ids)) c
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
GROUP BY t.cohort_definition_id,
	c.cohort_definition_id,
	tp.time_id;
*/