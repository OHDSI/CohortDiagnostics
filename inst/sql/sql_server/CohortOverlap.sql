SELECT DISTINCT u.target_cohort_id,
	u.comparator_cohort_id,
	u.num_persons_in_either AS either_subjects,
	u.num_persons_in_both AS both_subjects,
	u.num_persons_in_t_only AS t_only_subjects,
	u.num_persons_in_c_only AS c_only_subjects,
	i.num_persons_in_t_before_c AS t_before_c_subjects,
	i.num_persons_in_c_before_t AS c_before_t_subjects,
	i.num_persons_in_t_c_sameday AS same_day_subjects,
	i.num_persons_t_in_c AS t_in_c_subjects,
	i.num_persons_c_in_t AS c_in_t_subjects
FROM (
	SELECT t1.target_cohort_id,
		c1.comparator_cohort_id,
		COUNT(all_persons.subject_id) AS num_persons_in_either, --this is unique persons
		SUM(CASE 
				WHEN t1.subject_id IS NOT NULL
					AND c1.subject_id IS NOT NULL
					THEN 1
				ELSE 0
				END) AS num_persons_in_both,
		SUM(CASE 
				WHEN t1.subject_id IS NOT NULL
					AND c1.subject_id IS NULL
					THEN 1
				ELSE 0
				END) AS num_persons_in_t_only,
		SUM(CASE 
				WHEN t1.subject_id IS NULL
					AND c1.subject_id IS NOT NULL
					THEN 1
				ELSE 0
				END) AS num_persons_in_c_only
	FROM (
		SELECT DISTINCT subject_id
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (@target_cohort_ids)
			OR cohort_definition_id IN (@comparator_cohort_ids)
		) all_persons
	LEFT JOIN (
		SELECT DISTINCT cohort_definition_id target_cohort_id,
			subject_id
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (@target_cohort_ids)
		) t1 ON all_persons.subject_id = t1.subject_id
	LEFT JOIN (
		SELECT DISTINCT cohort_definition_id comparator_cohort_id,
			subject_id
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (@comparator_cohort_ids)
		) c1 ON all_persons.subject_id = c1.subject_id
	WHERE t1.target_cohort_id != c1.comparator_cohort_id
	GROUP BY t1.target_cohort_id,
		c1.comparator_cohort_id
	) u
LEFT JOIN (
	SELECT t1.target_cohort_id,
		c1.comparator_cohort_id,
		SUM(CASE 
				WHEN t1.min_start < c1.min_start
					THEN 1
				ELSE 0
				END) AS num_persons_in_t_before_c,
		SUM(CASE 
				WHEN c1.min_start < t1.min_start
					THEN 1
				ELSE 0
				END) AS num_persons_in_c_before_t,
		SUM(CASE 
				WHEN c1.min_start = t1.min_start
					THEN 1
				ELSE 0
				END) AS num_persons_in_t_c_sameday,
		SUM(CASE 
				WHEN t1.min_start >= c1.min_start
					AND t1.min_start <= c1.min_end
					THEN 1
				ELSE 0
				END) AS num_persons_t_in_c,
		SUM(CASE 
				WHEN c1.min_start >= t1.min_start
					AND c1.min_start <= t1.min_end
					THEN 1
				ELSE 0
				END) AS num_persons_c_in_t
	FROM (
		SELECT cohort_definition_id target_cohort_id,
			subject_id,
			MIN(cohort_start_date) AS min_start,
			MIN(cohort_end_date) AS min_end
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (@target_cohort_ids)
		GROUP BY cohort_definition_id,
			subject_id
		) t1
	INNER JOIN (
		SELECT cohort_definition_id comparator_cohort_id,
			subject_id,
			MIN(cohort_start_date) AS min_start,
			MIN(cohort_end_date) AS min_end
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (@comparator_cohort_ids)
		GROUP BY cohort_definition_id,
			subject_id
		) c1 ON t1.target_cohort_id = c1.comparator_cohort_id
		AND t1.subject_id = c1.subject_id
	WHERE t1.target_cohort_id != c1.comparator_cohort_id
	GROUP BY t1.target_cohort_id,
		c1.comparator_cohort_id
	) i ON u.target_cohort_id = i.target_cohort_id
	AND u.comparator_cohort_id = i.comparator_cohort_id
	ORDER BY u.target_cohort_id, u.comparator_cohort_id;