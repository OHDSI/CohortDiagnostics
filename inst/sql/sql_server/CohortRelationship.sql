SELECT t.cohort_definition_id cohort_id
	,c.cohort_definition_id comparator_cohort_id
	,@time_id time_id
	,COUNT_BIG(DISTINCT t.subject_id) both_subjects
	,-- present in both target and comparator
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) c_before_t_subjects
	,-- comparator cohort start date before target start date (offset)
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN t.subject_id
			ELSE NULL
			END) t_before_c_subjects
	,-- comparator cohort start date after target start date (offset)
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN t.subject_id
			ELSE NULL
			END) same_day_subjects
	,-- comparator cohort start date on target start date (offset)
	SUM(datediff(dd, CASE --max of target start (offset)/comparator start dates
				WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
					THEN c.cohort_start_date
				ELSE DATEADD(day, @start_day_offset, t.cohort_start_date)
				END, CASE -- min of target end (offset)/comparator end dates 
				WHEN c.cohort_end_date >= DATEADD(day, @end_day_offset, t.cohort_end_date)
					THEN DATEADD(day, @end_day_offset, t.cohort_end_date)
				ELSE c.cohort_end_date
				END) + 1) c_person_days
	,-- comparator cohort person days within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) c_subjects_start
	,-- comparator cohort subjects start within period (incidence)
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) c_subjects_exist
	,-- comparator cohort subjects exist within period (prevalence)
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) c_subjects_end
	,-- comparator cohort subjects terminate within period
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				AND c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) c_in_t_subjects -- comparator cohort records embedded within period
FROM #target_subset t
INNER JOIN #comparator_subset c ON c.subject_id = t.subject_id
	AND c.cohort_definition_id != t.cohort_definition_id
--	AND c.cohort_end_date >= t.cohort_start_date --DATEADD(day, @start_day_offset, t.cohort_start_date)
--	AND c.cohort_start_date <= t.cohort_end_date --DATEADD(day, @end_day_offset, t.cohort_end_date)
GROUP BY t.cohort_definition_id
	,c.cohort_definition_id;