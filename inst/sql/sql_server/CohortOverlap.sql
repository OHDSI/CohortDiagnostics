WITH target_cohorts
AS (
	SELECT cohort_definition_id target_cohort_id,
		subject_id,
		MIN(cohort_start_date) min_start,
		MIN(cohort_end_date) min_end
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@target_cohort_ids)
	GROUP BY cohort_definition_id,
		subject_id
	),
comparator_cohorts
AS (
	SELECT cohort_definition_id comparator_cohort_id,
		subject_id,
		MIN(cohort_start_date) min_start,
		MIN(cohort_end_date) min_end
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (@comparator_cohort_ids)
	GROUP BY cohort_definition_id,
		subject_id
	),
all_subjects
AS (
	SELECT DISTINCT subject_id
	FROM (
		SELECT DISTINCT subject_id
		FROM target_cohorts
		
		UNION
		
		SELECT DISTINCT subject_id
		FROM comparator_cohorts
		) subjects
	),
universe
AS (
	SELECT DISTINCT target_cohort_id,
		comparator_cohort_id,
		subject_id
	FROM (select distinct target_cohort_id from target_cohorts),
		(select distinct comparator_cohort_id from comparator_cohorts),
		all_subjects
	WHERE target_cohort_id != comparator_cohort_id
		AND target_cohort_id IS NOT NULL
		AND comparator_cohort_id IS NOT NULL
		AND target_cohort_id > 0
		AND comparator_cohort_id > 0
	),
overlap
AS (
	SELECT all1.target_cohort_id,
		all1.comparator_cohort_id,
		COUNT(DISTINCT CASE 
				WHEN all1.subject_id = t1.subject_id
					THEN t1.subject_id
				WHEN all1.subject_id = c1.subject_id
					THEN c1.subject_id
				ELSE NULL
				END) AS either_subjects, --this is unique persons
		COUNT(DISTINCT CASE 
				WHEN t1.subject_id IS NOT NULL
					AND c1.subject_id IS NOT NULL
					THEN t1.subject_id
				ELSE NULL
				END) AS both_subjects,
		COUNT(DISTINCT CASE 
				WHEN t1.subject_id IS NOT NULL
					AND c1.subject_id IS NULL
					THEN t1.subject_id
				ELSE NULL
				END) AS t_only_subjects,
		COUNT(DISTINCT CASE 
				WHEN t1.subject_id IS NULL
					AND c1.subject_id IS NOT NULL
					THEN c1.subject_id
				ELSE NULL
				END) AS c_only_subjects,
		COUNT(DISTINCT CASE 
				WHEN t1.min_start < c1.min_start
					THEN t1.subject_id
				ELSE NULL
				END) AS t_before_c_subjects,
		COUNT(DISTINCT CASE 
				WHEN c1.min_start < t1.min_start
					THEN c1.subject_id
				ELSE NULL
				END) AS c_before_t_subjects,
		COUNT(DISTINCT CASE 
				WHEN c1.min_start = t1.min_start
					THEN c1.subject_id
				ELSE NULL
				END) AS same_day_subjects,
		COUNT(DISTINCT CASE 
				WHEN t1.min_start >= c1.min_start
					AND t1.min_start <= c1.min_end
					THEN t1.subject_id
				ELSE NULL
				END) AS t_in_c_subjects,
		COUNT(DISTINCT CASE 
				WHEN c1.min_start >= t1.min_start
					AND c1.min_start <= t1.min_end
					THEN c1.subject_id
				ELSE NULL
				END) AS c_in_t_subjects
	FROM universe all1
	LEFT JOIN target_cohorts t1 ON all1.target_cohort_id = t1.target_cohort_id
		AND all1.subject_id = t1.subject_id
	LEFT JOIN comparator_cohorts c1 ON all1.comparator_cohort_id = c1.comparator_cohort_id
		AND all1.subject_id = c1.subject_id
	GROUP BY all1.target_cohort_id,
		all1.comparator_cohort_id
	)
SELECT DISTINCT target_cohort_id,
	comparator_cohort_id,
	either_subjects,
	both_subjects,
	t_only_subjects,
	c_only_subjects,
	t_before_c_subjects,
	c_before_t_subjects,
	same_day_subjects,
	t_in_c_subjects,
	c_in_t_subjects
FROM OVERLAP
ORDER BY target_cohort_id,
	comparator_cohort_id;
