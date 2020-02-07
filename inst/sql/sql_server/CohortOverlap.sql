{DEFAULT @cohort_database_schema = cdm_optum_extended_dod_v1027.ohdsi_results}
{DEFAULT @cohort_table = cohort}
{DEFAULT @target_cohort_id = 10481}
{DEFAULT @comparator_cohort_id = 12770}

SELECT union_counts.num_persons_in_either AS either_subjects,
	union_counts.num_persons_in_both AS both_subjects,
	union_counts.num_persons_in_t_only AS t_only_subjects,
	union_counts.num_persons_in_c_only AS c_only_subjects,
	intersection_counts.num_persons_in_t_before_c AS t_before_c_subjects,
	intersection_counts.num_persons_in_c_before_t AS c_before_t_subjects,
	intersection_counts.num_persons_in_t_c_sameday AS same_day_subjects,
	intersection_counts.num_persons_t_in_c AS t_in_c_subjects,
	intersection_counts.num_persons_c_in_t AS c_in_t_subjects
FROM (
	SELECT COUNT(all_persons.subject_id) AS num_persons_in_either,
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
		WHERE cohort_definition_id IN (
				@target_cohort_id,
				@comparator_cohort_id
				)
		) all_persons
	LEFT JOIN (
		SELECT DISTINCT subject_id
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id = @target_cohort_id
		) t1
		ON all_persons.subject_id = t1.subject_id
	LEFT JOIN (
		SELECT DISTINCT subject_id
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id = @comparator_cohort_id
		) c1
		ON all_persons.subject_id = c1.subject_id
	) union_counts,
	(
		SELECT SUM(CASE 
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
			SELECT subject_id,
				MIN(cohort_start_date) AS min_start,
				MIN(cohort_end_date) AS min_end
			FROM @cohort_database_schema.@cohort_table
			WHERE cohort_definition_id = @target_cohort_id
			GROUP BY subject_id
			) t1
		INNER JOIN (
			SELECT subject_id,
				MIN(cohort_start_date) AS min_start,
				MIN(cohort_end_date) AS min_end
			FROM @cohort_database_schema.@cohort_table
			WHERE cohort_definition_id = @comparator_cohort_id
			GROUP BY subject_id
			) c1
			ON t1.subject_id = c1.subject_id
		) intersection_counts;
