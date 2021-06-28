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
	WHERE cohort_definition_id IN (@target_cohort_ids)
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
	relationship_type,
	value
INTO #cohort_rel_long
FROM (
	-- first start occurrence of comparator relative to target
	--- temporal relationship: target cohort start date - comparator cohort start date. 
	---     negative values indicate that target cohort start date < comparator cohort
	---     positive values indicate that target cohort start date > comparator cohort
	SELECT t1.cohort_definition_id cohort_id,
		c1.cohort_definition_id comparator_cohort_id,
		CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
		'f' relationship_type, -- first occurrence (both target and comparator are first occurrence only)
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
	
	UNION
	
	-- all start occurrence of comparator relative to target
	--- temporal relationship: target cohort start date - comparator cohort start date. 
	---    negative values indicate that target cohort start date < comparator cohort
	---    positive values indicate that target cohort start date > comparator cohort
	SELECT t1.cohort_definition_id cohort_id,
		c1.cohort_definition_id comparator_cohort_id,
		CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
		'a' relationship_type, -- all relationship (both target and comparator all records)
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