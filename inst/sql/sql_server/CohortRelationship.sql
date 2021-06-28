IF OBJECT_ID('tempdb..#row_id_cs', 'U') IS NOT NULL
	DROP TABLE #row_id_cohort_sub;

IF OBJECT_ID('tempdb..#cohort_rel_long', 'U') IS NOT NULL
	DROP TABLE #cohort_rel_long;

--- Assign row_id_cs for each unique subject_id and cohort_start_date combination
--HINT DISTRIBUTE_ON_KEY(row_id_cs)
WITH cohort_data
AS (
	SELECT cohort_definition_id,
		subject_id,
		cohort_start_date,
		cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (
			@target_cohort_ids
			)
	),
cohort_first_occurrence
AS (
	SELECT cohort_definition_id,
		subject_id,
		MIN(cohort_start_date) cohort_start_date,
		MIN(cohort_end_date) cohort_end_date
	FROM cohort_data
	GROUP BY cohort_definition_id,
		subject_id
	),
row_id_cs
AS (
	SELECT ROW_NUMBER() OVER (
			ORDER BY subject_id ASC,
				cohort_start_date ASC
			) row_id_cs,
		cohort_definition_id,
		subject_id,
		cohort_start_date
	FROM (
		SELECT DISTINCT cohort_definition_id,
			subject_id,
			cohort_start_date
		FROM cohort_data
		) c
	)
SELECT rowid.row_id_cs,
	cd.cohort_definition_id,
	cd.subject_id,
	cd.cohort_start_date,
	cd.cohort_end_date,
	CASE 
		WHEN cd.cohort_start_date = fo.cohort_start_date
			THEN 1
		ELSE 0
		END first_occurrence
INTO #row_id_cohort_sub
FROM cohort_data cd
INNER JOIN row_id_cs rowid
	ON cd.cohort_definition_id = rowid.cohort_definition_id
		AND cd.subject_id = rowid.subject_id
		AND cd.cohort_start_date = rowid.cohort_start_date
INNER JOIN cohort_first_occurrence fo
	ON fo.cohort_definition_id = cd.cohort_definition_id
		AND fo.subject_id = cd.subject_id;

-- Inserting into final table
SELECT cohort_id,
	comparator_cohort_id,
	attribute_name,
	attribute_type,
	value
INTO #cohort_rel_long
FROM (
	-- calendar month
	-- incidence occurrence of subjects in cohort by calendar month
	SELECT cohort_definition_id cohort_id,
		0 comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), MONTH(cohort_start_date), 01) AS DATE) AS VARCHAR(30)) attribute_name,
		'mi' attribute_type, -- calendar month count (incidence) : monthly incidence
		COUNT_BIG(DISTINCT subject_id) value
	FROM #row_id_cohort_sub
	WHERE first_occurrence = 1
	GROUP BY cohort_definition_id,
		DATEFROMPARTS(YEAR(cohort_start_date), MONTH(cohort_start_date), 01)
	
	UNION
	
	-- calendar month
	-- occurrence of subjects in cohort by calendar month
	SELECT cohort_definition_id cohort_id,
		0 comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), MONTH(cohort_start_date), 01) AS DATE) AS VARCHAR(30)) attribute_name,
		'm' attribute_type, -- calendar month count (incidence) : monthly
		COUNT_BIG(DISTINCT subject_id) value
	FROM #row_id_cohort_sub
	GROUP BY cohort_definition_id,
		DATEFROMPARTS(YEAR(cohort_start_date), MONTH(cohort_start_date), 01)
	
	UNION -- calendar year
	
	-- incidence of subjects in cohort by calendar year
	SELECT cohort_definition_id cohort_id,
		0 comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), 01, 01) AS DATE) AS VARCHAR(30)) attribute_name,
		'yi' attribute_type, -- calendar year count (incidence) : yearly incidence
		COUNT_BIG(DISTINCT subject_id) value
	FROM #row_id_cohort_sub
	WHERE first_occurrence = 1
	GROUP BY cohort_definition_id,
		DATEFROMPARTS(YEAR(cohort_start_date), 01, 01)
	
	UNION
	
	-- incidence of subjects in cohort by calendar year
	SELECT cohort_definition_id cohort_id,
		0 comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), 01, 01) AS DATE) AS VARCHAR(30)) attribute_name,
		'y' attribute_type, -- calendar year count (incidence) : yearly
		COUNT_BIG(DISTINCT subject_id) value
	FROM #row_id_cohort_sub
	GROUP BY cohort_definition_id,
		DATEFROMPARTS(YEAR(cohort_start_date), 01, 01)
	
	UNION -- calendar quarter
	
	-- incidence of subjects in cohort by calendar quarter
	SELECT cohort_definition_id cohort_id,
		0 comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), CASE 
						WHEN MONTH(cohort_start_date) > 0
							AND MONTH(cohort_start_date) < 4
							THEN 1
						WHEN MONTH(cohort_start_date) > 3
							AND MONTH(cohort_start_date) < 7
							THEN 4
						WHEN MONTH(cohort_start_date) > 6
							AND MONTH(cohort_start_date) < 10
							THEN 7
						WHEN MONTH(cohort_start_date) > 10
							AND MONTH(cohort_start_date) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30)) attribute_name,
		'qi' attribute_type, -- calendar quarter count (incidence) : quarterly incidence
		COUNT_BIG(DISTINCT subject_id) value
	FROM #row_id_cohort_sub
	WHERE first_occurrence = 1
	GROUP BY cohort_definition_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), CASE 
						WHEN MONTH(cohort_start_date) > 0
							AND MONTH(cohort_start_date) < 4
							THEN 1
						WHEN MONTH(cohort_start_date) > 3
							AND MONTH(cohort_start_date) < 7
							THEN 4
						WHEN MONTH(cohort_start_date) > 6
							AND MONTH(cohort_start_date) < 10
							THEN 7
						WHEN MONTH(cohort_start_date) > 10
							AND MONTH(cohort_start_date) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30))
	
	UNION
	
	-- subjects in cohort by calendar quarter
	SELECT cohort_definition_id cohort_id,
		0 comparator_cohort_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), CASE 
						WHEN MONTH(cohort_start_date) > 0
							AND MONTH(cohort_start_date) < 4
							THEN 1
						WHEN MONTH(cohort_start_date) > 3
							AND MONTH(cohort_start_date) < 7
							THEN 4
						WHEN MONTH(cohort_start_date) > 6
							AND MONTH(cohort_start_date) < 10
							THEN 7
						WHEN MONTH(cohort_start_date) > 10
							AND MONTH(cohort_start_date) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30)) attribute_name,
		'q' AS attribute_type, -- calendar quarter count (incidence) : quarterly
		COUNT_BIG(DISTINCT subject_id) value
	FROM #row_id_cohort_sub
	GROUP BY cohort_definition_id,
		CAST(CAST(DATEFROMPARTS(YEAR(cohort_start_date), CASE 
						WHEN MONTH(cohort_start_date) > 0
							AND MONTH(cohort_start_date) < 4
							THEN 1
						WHEN MONTH(cohort_start_date) > 3
							AND MONTH(cohort_start_date) < 7
							THEN 4
						WHEN MONTH(cohort_start_date) > 6
							AND MONTH(cohort_start_date) < 10
							THEN 7
						WHEN MONTH(cohort_start_date) > 10
							AND MONTH(cohort_start_date) < 13
							THEN 10
						ELSE 1
						END, 01) AS DATE) AS VARCHAR(30))
	
	UNION -- first start occurrence of comparator relative to target
	
	--- temporal relationship: target cohort start date - comparator cohort start date. 
	---     negative values indicate that target cohort start date < comparator cohort
	---     positive values indicate that target cohort start date > comparator cohort
	SELECT t1.cohort_definition_id cohort_id,
		c1.cohort_definition_id comparator_cohort_id,
		CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
		'ri' attribute_type, -- relationship between cohorts, : relationship incidence
		COUNT_BIG(DISTINCT c1.row_id_cs) value -- the distinct here will not make a difference because first occurrence of comparator
		-- count of DISTINCT comparator cohort_start_date that meet the temporal criteria
	FROM #row_id_cohort_sub t1
	INNER JOIN #row_id_cohort_sub c1
		ON t1.subject_id = c1.subject_id
	WHERE t1.cohort_definition_id != c1.cohort_definition_id
		AND c1.first_occurrence = 1 --- first occurrence of comparator
	GROUP BY t1.cohort_definition_id,
		c1.cohort_definition_id,
		CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30))
	
	UNION -- all start occurrence of comparator relative to target
	
	--- temporal relationship: target cohort start date - comparator cohort start date. 
	---    negative values indicate that target cohort start date < comparator cohort
	---    positive values indicate that target cohort start date > comparator cohort
	SELECT t1.cohort_definition_id cohort_id,
		c1.cohort_definition_id comparator_cohort_id,
		CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
		'r' attribute_type, -- relationship between cohorts, : relationship
		COUNT_BIG(DISTINCT c1.row_id_cs) value -- the distinct here will lead to counting unique cohort_start_date for comparator
		-- count of DISTINCT comparator cohort_start_date that meet the temporal criteria
	FROM #row_id_cohort_sub t1
	INNER JOIN #row_id_cohort_sub c1
		ON t1.subject_id = c1.subject_id
	WHERE t1.cohort_definition_id != c1.cohort_definition_id
	GROUP BY t1.cohort_definition_id,
		c1.cohort_definition_id,
		CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30))
	) f;

IF OBJECT_ID('tempdb..#row_id_cs', 'U') IS NOT NULL
	DROP TABLE #row_id_cohort_sub;