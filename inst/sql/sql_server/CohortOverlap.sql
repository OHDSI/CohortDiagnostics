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
WHERE cohort_definition_id IN (@comparator_cohort_ids)
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
LEFT JOIN #target_cohorts t1 ON all1.target_cohort_id = t1.target_cohort_id
	AND all1.subject_id = t1.subject_id
LEFT JOIN #comparator_cohorts c1 ON all1.comparator_cohort_id = c1.comparator_cohort_id
	AND all1.subject_id = c1.subject_id
GROUP BY all1.target_cohort_id,
	all1.comparator_cohort_id;




-- Inserting into final table
SELECT cohort_id,
	comparator_cohort_id,
	attribute_name,
	attribute_type,
	value
INTO #cohort_overlap_long
FROM (
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'es'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		either_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'bs'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		both_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'ts'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		t_only_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'cs'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		c_only_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'tb'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		t_before_c_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'cb'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		c_before_t_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'sd'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		same_day_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'tc'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		t_in_c_subjects  AS value
	FROM #OVERLAP
	
	UNION
	
	SELECT DISTINCT target_cohort_id AS cohort_id,
		comparator_cohort_id,
		'ct'  AS attribute_name,
		'o'  AS attribute_type, -- overlap
		c_in_t_subjects  AS value
	FROM #OVERLAP
	
	-------------------------------------------------------------
	-------------------------------------------------------------
	
	UNION -- calendar month
	
	-- incidence of subjects in target cohort by calendar month
	SELECT target_cohort_id AS cohort_id,
		0  AS comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), MONTH(min_start), 01) AS DATE) AS VARCHAR(30))  AS attribute_name,
		'm'  AS attribute_type, -- calendar month count (incidence)
		COUNT_BIG(DISTINCT subject_id)  AS value
	FROM #target_cohorts
	GROUP BY target_cohort_id,
		DATEFROMPARTS(YEAR(min_start), MONTH(min_start), 01)
	
	UNION
	
	-- incidence of subjects in comparator cohort by calendar month
	SELECT comparator_cohort_id AS cohort_id,
		0  AS comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), MONTH(min_start), 01) AS DATE) AS VARCHAR(30))  AS attribute_name,
		'm'  AS attribute_type, -- calendar month count (incidence)
		COUNT_BIG(DISTINCT subject_id)  AS value
	FROM #comparator_cohorts
	GROUP BY comparator_cohort_id,
		DATEFROMPARTS(YEAR(min_start), MONTH(min_start), 1)
	
	UNION -- calendar year
	
	-- incidence of subjects in target cohort by calendar year
	SELECT target_cohort_id AS cohort_id,
		0  AS comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), 01, 01) AS DATE) AS VARCHAR(30))  AS attribute_name,
		'y'  AS attribute_type, -- calendar year count (incidence)
		COUNT_BIG(DISTINCT subject_id)  AS value
	FROM #target_cohorts
	GROUP BY target_cohort_id,
		DATEFROMPARTS(YEAR(min_start), 01, 01)
	
	UNION
	
	-- incidence of subjects in comparator cohort by calendar year
	SELECT comparator_cohort_id AS cohort_id,
		0  AS comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), 01, 01) AS DATE) AS VARCHAR(30))  AS attribute_name,
		'y'  AS attribute_type, -- calendar year count (incidence)
		COUNT_BIG(DISTINCT subject_id)  AS value
	FROM #comparator_cohorts
	GROUP BY comparator_cohort_id,
		DATEFROMPARTS(YEAR(min_start), 01, 1)
	
	UNION -- calendar quarter
	
	-- incidence of subjects in target cohort by calendar quarter
	SELECT target_cohort_id AS cohort_id,
		0  AS comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), CASE 
						WHEN MONTH(min_start) > 0
							AND MONTH(min_start) < 4
							THEN 1
						WHEN MONTH(min_start) > 3
							AND MONTH(min_start) < 7
							THEN 4
						WHEN MONTH(min_start) > 6
							AND MONTH(min_start) < 10
							THEN 7
						WHEN MONTH(min_start) > 10
							AND MONTH(min_start) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30))  AS attribute_name,
		'q'  AS attribute_type, -- calendar quarter count (incidence)
		COUNT_BIG(DISTINCT subject_id)  AS value
	FROM #target_cohorts
	GROUP BY target_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), CASE 
						WHEN MONTH(min_start) > 0
							AND MONTH(min_start) < 4
							THEN 1
						WHEN MONTH(min_start) > 3
							AND MONTH(min_start) < 7
							THEN 4
						WHEN MONTH(min_start) > 6
							AND MONTH(min_start) < 10
							THEN 7
						WHEN MONTH(min_start) > 10
							AND MONTH(min_start) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30))
	
	UNION
	
	-- incidence of subjects in comparator cohort by calendar quarter
	SELECT comparator_cohort_id AS cohort_id,
		0  AS comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), CASE 
						WHEN MONTH(min_start) > 0
							AND MONTH(min_start) < 4
							THEN 1
						WHEN MONTH(min_start) > 3
							AND MONTH(min_start) < 7
							THEN 4
						WHEN MONTH(min_start) > 6
							AND MONTH(min_start) < 10
							THEN 7
						WHEN MONTH(min_start) > 10
							AND MONTH(min_start) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30))  AS attribute_name,
		'q'  AS attribute_type, -- calendar quarter count (incidence)
		COUNT_BIG(DISTINCT subject_id)  AS value
	FROM #comparator_cohorts
	GROUP BY comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(min_start), CASE 
						WHEN MONTH(min_start) > 0
							AND MONTH(min_start) < 4
							THEN 1
						WHEN MONTH(min_start) > 3
							AND MONTH(min_start) < 7
							THEN 4
						WHEN MONTH(min_start) > 6
							AND MONTH(min_start) < 10
							THEN 7
						WHEN MONTH(min_start) > 10
							AND MONTH(min_start) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30))
	
	UNION -- temporal_relationship_long
	
	--- target cohort start date - comparator cohort start date. 
	--- negative values indicate that target cohort start date < comparator cohort
	--- positive values indicate that target cohort start date > comparator cohort
	
	SELECT t1.target_cohort_id  AS cohort_id,
		c1.comparator_cohort_id,
		CAST(FLOOR(DATEDIFF(dd, t1.min_start, c1.min_start) / 30) AS VARCHAR(30))  AS attribute_name,
		'r'  AS attribute_type, -- relationship between cohorts,
		COUNT_BIG(t1.subject_id)  AS value
	FROM #target_cohorts t1
	INNER JOIN #comparator_cohorts c1 ON t1.subject_id = c1.subject_id
	WHERE t1.target_cohort_id != c1.comparator_cohort_id
	GROUP BY t1.target_cohort_id,
		c1.comparator_cohort_id,
		CAST(FLOOR(DATEDIFF(dd, t1.min_start, c1.min_start) / 30) AS VARCHAR(30))
	) f;





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
