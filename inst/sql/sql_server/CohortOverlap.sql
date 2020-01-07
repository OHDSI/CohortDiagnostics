{DEFAULT @cohort_database_schema = cdm_optum_extended_dod_v1027.ohdsi_results}
{DEFAULT @cohort_table = cohort}
{DEFAULT @target_cohort_id = 10481}
{DEFAULT @comparator_cohort_id = 12770}


SELECT union_counts.num_persons_in_either,
	union_counts.num_persons_in_both,
	union_counts.num_persons_in_t_only,
	union_counts.num_persons_in_c_only,
	intersection_counts.num_persons_in_t_before_c,
	intersection_counts.num_persons_in_c_before_t,
	intersection_counts.num_persons_in_t_c_sameday
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
		SELECT subject_id,
			COUNT(DISTINCT cohort_definition_id) AS num_cohorts,
			MIN(cohort_start_date) AS min_start,
			MAX(cohort_end_date) AS max_end
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (
				@target_cohort_id,
				@comparator_cohort_id
				)
		GROUP BY subject_id
		) all_persons
	LEFT JOIN (
		SELECT subject_id,
			MIN(cohort_start_date) AS min_start,
			MAX(cohort_end_date) AS max_end
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (@target_cohort_id)
		GROUP BY subject_id
		) t1
		ON all_persons.subject_id = t1.subject_id
	LEFT JOIN (
		SELECT subject_id,
			MIN(cohort_start_date) AS min_start,
			MAX(cohort_end_date) AS max_end
		FROM @cohort_database_schema.@cohort_table
		WHERE cohort_definition_id IN (@comparator_cohort_id)
		GROUP BY subject_id
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
					END) AS num_persons_in_t_c_sameday
		FROM (
			SELECT subject_id,
				MIN(cohort_start_date) AS min_start,
				MAX(cohort_end_date) AS max_end
			FROM @cohort_database_schema.@cohort_table
			WHERE cohort_definition_id IN (@target_cohort_id)
			GROUP BY subject_id
			) t1
		INNER JOIN (
			SELECT subject_id,
				MIN(cohort_start_date) AS min_start,
				MAX(cohort_end_date) AS max_end
			FROM @cohort_database_schema.@cohort_table
			WHERE cohort_definition_id IN (@comparator_cohort_id)
			GROUP BY subject_id
			) c1
			ON t1.subject_id = c1.subject_id
		) intersection_counts;
