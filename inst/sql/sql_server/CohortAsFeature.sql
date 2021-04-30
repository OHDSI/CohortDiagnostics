SELECT a.cohort_definition_id AS cohort_id,
	b.cohort_definition_id AS feature_cohort_id,
	-- feature cohort start date is the same as target cohort start date
	COUNT(DISTINCT CASE 
			WHEN b.cohort_start_date = a.cohort_start_date
				THEN b.cohort_definition_id
			END) AS same_day_subjects,
	COUNT(CASE 
			WHEN b.cohort_start_date = a.cohort_start_date
				THEN b.cohort_definition_id
			END) AS same_day_records,
	-- feature cohort start date is before target cohort start date
	MAX(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_end_date))
			END) AS before_days_max,
	MIN(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			END) AS before_days_min,
	AVG(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			END) AS before_days_avg,
	SUM(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN 1
			ELSE 0
			END) AS before_days_sum,
	COUNT_BIG(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN b.cohort_definition_id
			END) AS before_days_count,
	STDEV(CASE 
			WHEN b.cohort_start_date < a.cohort_start_date
				THEN (DATEDIFF(dd, b.cohort_start_date, a.cohort_start_date))
			END) AS before_days_stdev,
	-- feature cohort start date is on or after target cohort end date
	MAX(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS after_days_max,
	MIN(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS after_days_min,
	AVG(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS after_days_avg,
	SUM(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN 1
			ELSE 0
			END) AS after_days_sum,
	STDEV(CASE 
			WHEN b.cohort_start_date > a.cohort_end_date
				THEN (DATEDIFF(dd, a.cohort_end_date, b.cohort_start_date))
			END) AS after_days_stdev,
	COUNT_BIG(CASE 
			WHEN b.cohort_start_date > a.cohort_start_date
				THEN b.cohort_definition_id
			END) AS after_days_count,
	-- feature cohort start date is between (including) target cohort start date and end date
	MAX(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_end_date))
			END) AS during_days_max,
	MIN(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_start_date))
			END) AS during_days_min,
	AVG(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN (DATEDIFF(dd, a.cohort_start_date, b.cohort_start_date))
			END) AS during_days_avg,
	SUM(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN 1
			ELSE 0
			END) AS during_days_sum,
	STDEV(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN 1
			ELSE 0
			END) AS during_days_stdev,
	COUNT_BIG(CASE 
			WHEN b.cohort_start_date >= a.cohort_start_date
				AND a.cohort_end_date >= b.cohort_start_date
				THEN b.cohort_definition_id
			END) AS during_days_count
FROM @cohort_database_schema.@cohort_table a
CROSS JOIN @cohort_database_schema.@feature_cohort_table b
INNER JOIN #cohort_combis c ON a.cohort_definition_id = c.target_cohort_id
	AND b.cohort_definition_id = c.comparator_cohort_id
WHERE a.subject_id = b.subject_id
	AND a.cohort_definition_id != b.cohort_definition_id
GROUP BY a.cohort_definition_id,
	b.cohort_definition_id;