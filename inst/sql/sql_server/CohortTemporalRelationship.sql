IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;

IF OBJECT_ID('tempdb..#cohort_rel_long', 'U') IS NOT NULL
	DROP TABLE #cohort_rel_long;

--- Assign row_id_cs for each unique subject_id and cohort_start_date combination
--HINT DISTRIBUTE_ON_KEY(row_id_cs)
WITH cohort_data
AS (
	SELECT ROW_NUMBER() OVER (
			ORDER BY subject_id ASC,
				cohort_start_date ASC
			) row_id_cs,
		cohort_definition_id,
		subject_id,
		cohort_start_date,
		cohort_end_date
	FROM @cohort_database_schema.@cohort_table
	WHERE cohort_definition_id IN (
			@target_cohort_ids,
			@comparator_cohort_ids
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
	)
SELECT cd.row_id_cs,
	cd.cohort_definition_id,
	cd.subject_id,
	cd.cohort_start_date,
	cd.cohort_end_date,
	CASE 
		WHEN cd.cohort_start_date = fo.cohort_start_date
			THEN 1
		ELSE 0
		END first_occurrence
INTO #cohort_row_id
FROM cohort_data cd
INNER JOIN cohort_first_occurrence fo
	ON fo.cohort_definition_id = cd.cohort_definition_id
		AND fo.subject_id = cd.subject_id;

IF OBJECT_ID('tempdb..#cohort_rel_long', 'U') IS NOT NULL
	DROP TABLE #cohort_rel_long;

CREATE TABLE #cohort_rel_long (
	cohort_id BIGINT,
	comparator_cohort_id BIGINT,
	attribute_name VARCHAR,
	relationship_type VARCHAR,
	subjects FLOAT,
	records FLOAT
	);

--- temporal relationship: target cohort start date - comparator cohort start date. 
---     negative values indicate that target cohort start date < comparator cohort
---     positive values indicate that target cohort start date > comparator cohort
INSERT INTO #cohort_rel_long (
	cohort_id,
	comparator_cohort_id,
	attribute_name,
	relationship_type,
	subjects,
	records
	)
SELECT t1.cohort_definition_id cohort_id,
	c1.cohort_definition_id comparator_cohort_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
	'S1S1' relationship_type, -- first target start date, first comparator start date
	COUNT_BIG(DISTINCT c1.cohort_definition_id) subjects, -- the distinct here will not make a difference because first occurrence of comparator
	COUNT_BIG(DISTINCT c1.row_id_cs) records
	-- count of DISTINCT comparator cohort_start_date that meet the temporal criteria
FROM #cohort_row_id t1
INNER JOIN #cohort_row_id c1
	ON t1.subject_id = c1.subject_id
WHERE t1.cohort_definition_id != c1.cohort_definition_id
	AND t1.first_occurrence = 1 --- first occurrence of comparator
	AND c1.first_occurrence = 1 --- first occurrence of target
	AND t1.cohort_definition_id IN (@target_cohort_ids)
	AND c1.cohort_definition_id IN (@comparator_cohort_ids)
GROUP BY t1.cohort_definition_id,
	c1.cohort_definition_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30));

INSERT INTO #cohort_rel_long (
	cohort_id,
	comparator_cohort_id,
	attribute_name,
	relationship_type,
	subjects,
	records
	)
SELECT t1.cohort_definition_id cohort_id,
	c1.cohort_definition_id comparator_cohort_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
	'S1SA' relationship_type, -- first target start date, all comparator start date
	COUNT_BIG(DISTINCT c1.cohort_definition_id) subjects, -- the distinct here will not make a difference because first occurrence of comparator
	COUNT_BIG(DISTINCT c1.row_id_cs) records
	-- count of DISTINCT comparator cohort_start_date that meet the temporal criteria
FROM #cohort_row_id t1
INNER JOIN #cohort_row_id c1
	ON t1.subject_id = c1.subject_id
WHERE t1.cohort_definition_id != c1.cohort_definition_id
	AND t1.first_occurrence = 1 --- first occurrence of target
	AND t1.cohort_definition_id IN (@target_cohort_ids)
	AND c1.cohort_definition_id IN (@comparator_cohort_ids)
GROUP BY t1.cohort_definition_id,
	c1.cohort_definition_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30));

INSERT INTO #cohort_rel_long (
	cohort_id,
	comparator_cohort_id,
	attribute_name,
	relationship_type,
	subjects,
	records
	)
SELECT t1.cohort_definition_id cohort_id,
	c1.cohort_definition_id comparator_cohort_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
	'SAS1' relationship_type, -- All target start date, first comparator start date
	COUNT_BIG(DISTINCT c1.cohort_definition_id) subjects, -- the distinct here will not make a difference because first occurrence of comparator
	-- count of DISTINCT comparator cohort_start_date that meet the temporal criteria
	COUNT_BIG(DISTINCT c1.row_id_cs) records
FROM #cohort_row_id t1
INNER JOIN #cohort_row_id c1
	ON t1.subject_id = c1.subject_id
WHERE t1.cohort_definition_id != c1.cohort_definition_id
	AND c1.first_occurrence = 1 --- first occurrence of comparator
	AND t1.cohort_definition_id IN (@target_cohort_ids)
	AND c1.cohort_definition_id IN (@comparator_cohort_ids)
GROUP BY t1.cohort_definition_id,
	c1.cohort_definition_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30));

INSERT INTO #cohort_rel_long (
	cohort_id,
	comparator_cohort_id,
	attribute_name,
	relationship_type,
	subjects,
	records
	)
SELECT t1.cohort_definition_id cohort_id,
	c1.cohort_definition_id comparator_cohort_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30)) attribute_name, -- date diff
	'SASA' relationship_type, -- All target start date, all compartor start date
	COUNT_BIG(DISTINCT c1.cohort_definition_id) subjects, -- the distinct here will not make a difference because first occurrence of comparator
	-- count of DISTINCT comparator cohort_start_date that meet the temporal criteria
	COUNT_BIG(DISTINCT c1.row_id_cs) records
FROM #cohort_row_id t1
INNER JOIN #cohort_row_id c1
	ON t1.subject_id = c1.subject_id
WHERE t1.cohort_definition_id != c1.cohort_definition_id
	AND t1.cohort_definition_id IN (@target_cohort_ids)
	AND c1.cohort_definition_id IN (@comparator_cohort_ids)
GROUP BY t1.cohort_definition_id,
	c1.cohort_definition_id,
	CAST(FLOOR(DATEDIFF(dd, t1.cohort_start_date, c1.cohort_start_date) / 30) AS VARCHAR(30));;

IF OBJECT_ID('tempdb..#cohort_row_id', 'U') IS NOT NULL
	DROP TABLE #cohort_row_id;
