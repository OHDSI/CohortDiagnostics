IF OBJECT_ID('tempdb..#cohort_rel', 'U') IS NOT NULL
	DROP TABLE #cohort_rel;

SELECT t.cohort_definition_id cohort_id,
	c.cohort_definition_id comparator_cohort_id,
	tp.time_id,
	COUNT_BIG(t.subject_id) target_records, -- target cohort records in time period
	COUNT_BIG(DISTINCT t.subject_id) target_subjects, -- target cohort subjects in time period
	COUNT_BIG(c.subject_id) comparator_records, -- comparator cohort records in time period (includes overlap)
	COUNT_BIG(DISTINCT c.subject_id) comparator_subjects, -- comparator cohort subjects in time period (includes overlap)
	COUNT_BIG(CASE 
			WHEN t.subject_id IS NOT NULL
				AND c.subject_Id IS NOT NULL
				THEN t.subject_id
			ELSE NULL
			END) both_records, -- present in both target and comparator
	COUNT_BIG(DISTINCT CASE 
			WHEN t.subject_id IS NOT NULL
				AND c.subject_Id IS NOT NULL
				THEN t.subject_id
			ELSE NULL
			END) both_subjects, -- present in both target and comparator
	COUNT_BIG(CASE 
			WHEN t.subject_id IS NOT NULL
				AND c.subject_Id IS NULL
				THEN t.subject_id
			ELSE NULL
			END) t_records_only,
	COUNT_BIG(DISTINCT CASE 
			WHEN t.subject_id IS NOT NULL
				AND c.subject_Id IS NULL
				THEN t.subject_id
			ELSE NULL
			END) t_subjects_only,
	COUNT_BIG(CASE 
			WHEN c.cohort_start_date < DATEADD(day, tp.start_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_before_t_records, -- comparator cohorts records start before target (offset) in period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, tp.start_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_before_t_subjects, -- comparator cohorts records start before target (offset) in period
	COUNT_BIG(CASE 
			WHEN c.cohort_start_date > DATEADD(day, tp.start_day, t.cohort_start_date)
				THEN t.subject_id
			ELSE NULL
			END) t_before_c_records, -- target cohorts (offset) records start before comparator in period,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, tp.start_day, t.cohort_start_date)
				THEN t.subject_id
			ELSE NULL
			END) t_before_c_subjects, -- target cohorts (offset) records start before comparator in period
	COUNT_BIG(CASE 
			WHEN c.cohort_start_date = DATEADD(day, tp.start_day, t.cohort_start_date)
				THEN t.subject_id
			ELSE NULL
			END) same_day_records, -- target cohorts (offset) subjects start on comparator cohort start in period,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, tp.start_day, t.cohort_start_date)
				THEN t.subject_id
			ELSE NULL
			END) same_day_subjects, -- target cohorts (offset) subjects start on comparator cohort start in period
	SUM(datediff(dd, CASE 
				WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
					THEN c.cohort_start_date
				ELSE DATEADD(day, tp.start_day, t.cohort_start_date)
				END, CASE 
				WHEN c.cohort_end_date >= DATEADD(day, tp.end_day, t.cohort_start_date)
					THEN DATEADD(day, tp.end_day, t.cohort_start_date)
				ELSE c.cohort_end_date
				END) + 1) c_person_days, -- comparator cohort person days within period
	COUNT_BIG(CASE 
			WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_records_start, -- comparator cohorts records start within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_subjects_start, -- comparator cohort subjects start within period (incidence)
	COUNT_BIG(CASE 
			WHEN c.cohort_end_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_records_end, -- comparator cohort records terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_subjects_end, -- comparator cohort subjects terminate within period
	COUNT_BIG(CASE 
			WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				AND c.cohort_end_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_in_t_records, -- comparator cohort records embedded within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				AND c.cohort_end_date >= DATEADD(day, tp.start_day, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, tp.end_day, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_in_t_subjects -- comparator cohort records embedded within period
INTO #cohort_rel
FROM #time_periods tp -- offset
CROSS JOIN @cohort_database_schema.@cohort_table t
LEFT JOIN @cohort_database_schema.@cohort_table c
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
