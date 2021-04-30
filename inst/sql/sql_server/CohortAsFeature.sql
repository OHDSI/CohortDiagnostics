SELECT a.cohort_definition_id AS cohort_id,
	b.cohort_definition_id AS feature_cohort_id,
	-- feature cohort start date relationship
	-- feature cohort start date is the same as target cohort start date
	COUNT_BIG(DISTINCT CASE 
			WHEN b.cohort_start_date = a.cohort_start_date
				THEN b.subject_id
			END) AS fs_same_subjects, -- count of subjects in feature cohort prior to start date of target cohort
	-- feature cohort start date is before target cohort start date
	MAX(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			END) AS fs_before_max, -- max days difference between start date of feature cohort and start date of target cohort
	MIN(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			END) AS fs_before_min, -- min days difference between start date of feature cohort and start date of target cohort
	AVG(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			END) AS fs_before_avg, -- avg days difference between start date of feature cohort and start date of target cohort
	SUM(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			ELSE 0
			END) AS fs_before_sum, -- sum days difference between start date of feature cohort and start date of target cohort
	COUNT_BIG(DISTINCT CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN b.subject_id
			END) AS fs_before_count, -- count of subjects in feature cohort with cohort start date before start date of target cohort
	STDEV(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			END) AS fs_before_stdev,-- standard deviation of days difference between start date of feature cohort and start date of target cohort
	-- feature cohort start date is after target cohort end date
	MAX(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS fs_after_max,-- max days difference between start date of feature cohort and start date of target cohort
	MIN(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS fs_after_min,-- min days difference between start date of feature cohort and start date of target cohort
	AVG(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS fs_after_avg,-- avg days difference between start date of feature cohort and start date of target cohort
	SUM(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			ELSE 0
			END) AS fs_after_sum,-- sum days difference between start date of feature cohort and start date of target cohort
	STDEV(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS fs_after_stdev,-- std diff days difference between start date of feature cohort and start date of target cohort
	COUNT_BIG(DISTINCT CASE 
			WHEN b.cohort_start_date > a.cohort_start_date
				THEN b.subject_id
			END) AS fs_after_count,-- count of subjects in feature cohort with cohort start date before start date of target cohort
	-- feature cohort start date is between (including) target cohort start date and end date
	MAX(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS fs_during_max,-- max days difference between start date of feature cohort and start date of target cohort
	MIN(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_start_date))
			END) AS fs_during_min,-- min days difference between start date of feature cohort and start date of target cohort
	AVG(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_start_date))
			END) AS fs_during_avg,-- avg days difference between start date of feature cohort and start date of target cohort
	SUM(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_start_date))
			ELSE 0
			END) AS fs_during_sum,-- sum days difference between start date of feature cohort and start date of target cohort
	STDEV(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_start_date))
			END) AS fs_during_stdev,-- std deviation days difference between start date of feature cohort after end date of target cohort
	COUNT_BIG(DISTINCT CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN b.subject_id
			END) AS fs_during_count,-- count of subjects in feature cohort with cohort start date after end date of target cohort
	-- feature cohort end date relationship
	-- feature cohort end date is the same as target cohort start date
	COUNT_BIG(DISTINCT CASE 
			WHEN b.cohort_end_date = a.cohort_start_date
				THEN b.subject_id
			END) AS fe_same_subjects,-- count of subjects in feature cohort with cohort start date and start date of target cohort
	-- feature cohort end date is before target cohort start date
	MAX(CASE 
			WHEN b.cohort_end_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_end_date, a.cohort_start_date))
			END) AS fe_before_max,-- max of days difference when feature cohort end date if before start date of target cohort
	MIN(CASE 
			WHEN b.cohort_end_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_end_date, a.cohort_start_date))
			END) AS fe_before_min,-- min of days difference when feature cohort end date if before start date of target cohort
	AVG(CASE 
			WHEN b.cohort_end_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_end_date, a.cohort_start_date))
			END) AS fe_before_avg,-- avg of days difference when feature cohort end date if before start date of target cohort
	SUM(CASE 
			WHEN b.cohort_end_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_end_date, a.cohort_start_date))
			END) AS fe_before_sum,-- sum of days difference when feature cohort end date if before start date of target cohort
	COUNT_BIG(CASE 
			WHEN b.cohort_end_date < a.cohort_start_date
				THEN b.subject_id
			END) AS fe_before_count,-- count of subjects where feature cohort end date if before start date of target cohort
	STDEV(CASE 
			WHEN b.cohort_end_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_end_date, a.cohort_start_date))
			END) AS fe_before_stdev,-- std deviation of days difference when feature cohort end date if before start date of target cohort
	-- feature cohort end date is after target cohort end date
	MAX(CASE 
			WHEN b.cohort_end_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_end_date))
			END) AS fe_after_max,-- max of days difference when feature cohort end date is after end date of target cohort
	MIN(CASE 
			WHEN b.cohort_end_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_end_date))
			END) AS fe_after_min,-- min of days difference when feature cohort end date is after end date of target cohort
	AVG(CASE 
			WHEN b.cohort_end_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_end_date))
			END) AS fe_after_avg,-- avg of days difference when feature cohort end date is after end date of target cohort
	SUM(CASE 
			WHEN b.cohort_end_date > a.cohort_end_date
				THEN 1
			ELSE 0
			END) AS fe_after_sum,-- sum of days difference when feature cohort end date is after end date of target cohort
	STDEV(CASE 
			WHEN b.cohort_end_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_end_date))
			END) AS fe_after_stdev,-- std deviation of days difference when feature cohort end date is after end date of target cohort
	COUNT_BIG(DISTINCT CASE 
			WHEN b.cohort_end_date > a.cohort_end_date
				THEN b.subject_id
			END) AS fe_after_count,-- count subjects when feature cohort end date is after end date of target cohort
	-- feature cohort end date is between (including) target cohort start date and end date
	MAX(CASE 
			WHEN b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS fe_during_max,
	MIN(CASE 
			WHEN b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS fe_during_min,
	AVG(CASE 
			WHEN b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS fe_during_avg,
	SUM(CASE 
			WHEN b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN 1
			ELSE 0
			END) AS fe_during_sum,
	STDEV(CASE 
			WHEN b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN 1
			ELSE 0
			END) AS fe_during_stdev,
	COUNT_BIG(DISTINCT CASE 
			WHEN b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN b.subject_id
			END) AS fe_during_count,
	-- feature cohort start & end date relationship
	-- feature cohort start date and end date are between (including) target cohort start date and end date
	MAX(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_start_date >= b.cohort_end_date
				AND b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS fo_during_max,
	MIN(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_start_date >= b.cohort_end_date
				AND b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS fo_during_min,
	AVG(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_start_date >= b.cohort_end_date
				AND b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS fo_during_avg,
	SUM(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_start_date >= b.cohort_end_date
				AND b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN 1
			ELSE 0
			END) AS fo_during_sum,
	STDEV(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_start_date >= b.cohort_end_date
				AND b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN 1
			ELSE 0
			END) AS fo_during_stdev,
	COUNT_BIG(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_start_date >= b.cohort_end_date
				AND b.cohort_end_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_end_date
				THEN b.subject_id
			END) AS fo_during_count
FROM @cohort_database_schema.@cohort_table a
CROSS JOIN @cohort_database_schema.@feature_cohort_table b
INNER JOIN #cohort_combis c ON a.cohort_definition_id = c.target_cohort_id
	AND b.cohort_definition_id = c.comparator_cohort_id
WHERE a.subject_id = b.subject_id
	AND a.cohort_definition_id != b.cohort_definition_id
GROUP BY a.cohort_definition_id,
	b.cohort_definition_id;