IF OBJECT_ID('tempdb..#target_cohorts', 'U') IS NOT NULL
	DROP TABLE #target_cohorts;

IF OBJECT_ID('tempdb..#comparator_cohorts', 'U') IS NOT NULL
	DROP TABLE #comparator_cohorts;

IF OBJECT_ID('tempdb..#all_subjects', 'U') IS NOT NULL
	DROP TABLE #all_subjects;

IF OBJECT_ID('tempdb..#universe', 'U') IS NOT NULL
	DROP TABLE #universe;

IF OBJECT_ID('tempdb..#overlap', 'U') IS NOT NULL
	DROP TABLE #overlap;

IF OBJECT_ID('tempdb..#cohort_overlap_long', 'U') IS NOT NULL
	DROP TABLE #cohort_overlap_long;

--- First occurrence of target cohorts
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT cohort_definition_id target_cohort_id,
	subject_id,
	MIN(cohort_start_date) min_start,
	MIN(cohort_end_date) min_end
INTO #target_cohorts
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id IN (@target_cohort_ids)
GROUP BY cohort_definition_id,
	subject_id;

--- First occurrence of comparator cohorts
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT cohort_definition_id comparator_cohort_id,
	subject_id,
	MIN(cohort_start_date) min_start,
	MIN(cohort_end_date) min_end
INTO #comparator_cohorts
FROM @cohort_database_schema.@cohort_table
GROUP BY cohort_definition_id,
	subject_id;

--- get all subjects in either target or comparator
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT DISTINCT subject_id
INTO #all_subjects
FROM (
	SELECT DISTINCT subject_id
	FROM #target_cohorts
	
	UNION
	
	SELECT DISTINCT subject_id
	FROM #comparator_cohorts
	) subjects;

--- create the universe of all combis of target_cohort_id, comparator_cohort_id and subject_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT DISTINCT target_cohort_id,
	comparator_cohort_id,
	subject_id
INTO #universe
FROM (
	SELECT DISTINCT target_cohort_id
	FROM #target_cohorts
	) target,
	(
		SELECT DISTINCT comparator_cohort_id
		FROM #comparator_cohorts
		) comparator,
	#all_subjects subjects
WHERE target_cohort_id != comparator_cohort_id
	AND target_cohort_id IS NOT NULL
	AND comparator_cohort_id IS NOT NULL
	AND target_cohort_id > 0
	AND comparator_cohort_id > 0;

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
INTO #overlap
FROM #universe all1
LEFT JOIN #target_cohorts t1
	ON all1.target_cohort_id = t1.target_cohort_id
		AND all1.subject_id = t1.subject_id
LEFT JOIN #comparator_cohorts c1
	ON all1.comparator_cohort_id = c1.comparator_cohort_id
		AND all1.subject_id = c1.subject_id
GROUP BY all1.target_cohort_id,
	all1.comparator_cohort_id;

IF OBJECT_ID('tempdb..#target_cohorts', 'U') IS NOT NULL
	DROP TABLE #target_cohorts;

IF OBJECT_ID('tempdb..#comparator_cohorts', 'U') IS NOT NULL
	DROP TABLE #comparator_cohorts;

IF OBJECT_ID('tempdb..#all_subjects', 'U') IS NOT NULL
	DROP TABLE #all_subjects;

IF OBJECT_ID('tempdb..#universe', 'U') IS NOT NULL
	DROP TABLE #universe;

IF OBJECT_ID('tempdb..#cohort_overlap_long', 'U') IS NOT NULL
	DROP TABLE #cohort_overlap_long;

CREATE TABLE #cohort_overlap_long (
	cohort_id BIGINT,
	comparator_cohort_id BIGINT,
	attribute_name VARCHAR,
	count_value FLOAT
	);

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'es' AS attribute_name,
	either_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'bs' AS attribute_name,
	both_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'ts' AS attribute_name,
	t_only_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'cs' AS attribute_name,
	c_only_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'tb' AS attribute_name,
	t_before_c_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'cb' AS attribute_name,
	c_before_t_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'sd' AS attribute_name,
	same_day_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'tc' AS attribute_name,
	t_in_c_subjects count_value
FROM #OVERLAP;

INSERT INTO #cohort_overlap_long
SELECT DISTINCT target_cohort_id AS cohort_id,
	comparator_cohort_id,
	'ct' AS attribute_name,
	c_in_t_subjects count_value
FROM #OVERLAP;

IF OBJECT_ID('tempdb..#target_cohorts', 'U') IS NOT NULL
	DROP TABLE #target_cohorts;

IF OBJECT_ID('tempdb..#comparator_cohorts', 'U') IS NOT NULL
	DROP TABLE #comparator_cohorts;

IF OBJECT_ID('tempdb..#all_subjects', 'U') IS NOT NULL
	DROP TABLE #all_subjects;

IF OBJECT_ID('tempdb..#universe', 'U') IS NOT NULL
	DROP TABLE #universe;

IF OBJECT_ID('tempdb..#overlap', 'U') IS NOT NULL
	DROP TABLE #overlap;