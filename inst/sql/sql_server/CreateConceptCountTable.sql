{DEFAULT @table_is_temp = FALSE}
{DEFAULT @use_achilles = FALSE}

{@table_is_temp} ? {
IF OBJECT_ID('tempdb..@concept_counts_table', 'U') IS NOT NULL
  DROP TABLE @concept_counts_table;
} : {
IF OBJECT_ID('@work_database_schema.@concept_counts_table', 'U') IS NOT NULL
	DROP TABLE @work_database_schema.@concept_counts_table;
}

SELECT concept_id,
	concept_count,
	concept_subjects
{@table_is_temp} ? {
INTO @concept_counts_table
} : { 
INTO @work_database_schema.@concept_counts_table
}

{@use_achilles} ? {

-- Achilles analysis ids used for creating the concept counts table
-- condition:    400 (persons),  401 (standard concepts),  425 (source concepts)
-- drug:         700 (persons),  701 (standard concepts),  725 (source concepts)
-- procedure:    600 (persons),  601 (standard concepts),  625 (source concepts)
-- measurement: 1800 (persons), 1801 (standard concepts), 1825 (source concepts)
-- observation:  800 (persons),  801 (standard concepts),  825 (source concepts)
FROM (
	SELECT 
		CAST(stratum_1 AS INT) AS concept_id,
		count_value AS concept_count
	FROM @achilles_database_schema.achilles_results
	WHERE analysis_id IN (401,601,701,801,1801,425,625,725,825,1825) AND stratum_1 != '0'
) q1
LEFT JOIN 
(
	SELECT 
		CAST(stratum_1 AS INT) AS concept_id,
		count_value AS concept_subjects
	FROM  @achilles_database_schema.achilles_results
	WHERE analysis_id IN (400,600,700,800,1800) AND stratum_1 != '0'
) q2
ON q1.concept_id = q2.concept_id

} : {

FROM (
	SELECT condition_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT co.person_id) AS concept_subjects
	FROM @cdm_database_schema.condition_occurrence co
	JOIN @cdm_database_schema.observation_period op 
  ON co.person_id = op.person_id
  AND co.condition_start_date >= op.observation_period_start_date
  AND co.condition_start_date <= op.observation_period_end_date
	GROUP BY condition_concept_id
	
	UNION ALL
	
	SELECT condition_source_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT co.person_id) AS concept_subjects
	FROM @cdm_database_schema.condition_occurrence co
	JOIN @cdm_database_schema.observation_period op 
  ON co.person_id = op.person_id
  AND co.condition_start_date >= op.observation_period_start_date
  AND co.condition_start_date <= op.observation_period_end_date
	GROUP BY condition_source_concept_id
	
	UNION ALL
	
	SELECT drug_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT de.person_id) AS concept_subjects
	FROM @cdm_database_schema.drug_exposure de
	JOIN @cdm_database_schema.observation_period op 
  ON de.person_id = op.person_id
  AND de.drug_exposure_start_date >= op.observation_period_start_date
  AND de.drug_exposure_start_date <= op.observation_period_end_date
	GROUP BY drug_concept_id
	
	UNION ALL
	
	SELECT drug_source_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT de.person_id) AS concept_subjects
	FROM @cdm_database_schema.drug_exposure de
	JOIN @cdm_database_schema.observation_period op 
  ON de.person_id = op.person_id
  AND de.drug_exposure_start_date >= op.observation_period_start_date
  AND de.drug_exposure_start_date <= op.observation_period_end_date
	GROUP BY drug_source_concept_id
	
	UNION ALL
	
	SELECT procedure_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT po.person_id) AS concept_subjects
	FROM @cdm_database_schema.procedure_occurrence po
	JOIN @cdm_database_schema.observation_period op 
  ON po.person_id = op.person_id
  AND po.procedure_date >= op.observation_period_start_date
  AND po.procedure_date <= op.observation_period_end_date
	GROUP BY procedure_concept_id
	
	UNION ALL
	
	SELECT procedure_source_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT po.person_id) AS concept_subjects
	FROM @cdm_database_schema.procedure_occurrence po
	JOIN @cdm_database_schema.observation_period op 
  ON po.person_id = op.person_id
  AND po.procedure_date >= op.observation_period_start_date
  AND po.procedure_date <= op.observation_period_end_date
	GROUP BY procedure_source_concept_id
	
	UNION ALL
	
	SELECT measurement_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT m.person_id) AS concept_subjects
	FROM @cdm_database_schema.measurement m
	JOIN @cdm_database_schema.observation_period op 
  ON m.person_id = op.person_id
  AND m.measurement_date >= op.observation_period_start_date
  AND m.measurement_date <= op.observation_period_end_date
	GROUP BY measurement_concept_id
	
	UNION ALL
  
	SELECT measurement_source_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT m.person_id) AS concept_subjects
	FROM @cdm_database_schema.measurement m
	JOIN @cdm_database_schema.observation_period op 
  ON m.person_id = op.person_id
  AND m.measurement_date >= op.observation_period_start_date
  AND m.measurement_date <= op.observation_period_end_date
	GROUP BY measurement_source_concept_id
	
	UNION ALL
	
	SELECT observation_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT o.person_id) AS concept_subjects
	FROM @cdm_database_schema.observation o
	JOIN @cdm_database_schema.observation_period op 
  ON o.person_id = op.person_id
  AND o.observation_date >= op.observation_period_start_date
  AND o.observation_date <= op.observation_period_end_date
	GROUP BY observation_concept_id
	
	UNION ALL
	
	SELECT observation_source_concept_id AS concept_id,
		COUNT_BIG(*) AS concept_count,
		COUNT_BIG(DISTINCT o.person_id) AS concept_subjects
	FROM @cdm_database_schema.observation o
	JOIN @cdm_database_schema.observation_period op 
  ON o.person_id = op.person_id
  AND o.observation_date >= op.observation_period_start_date
  AND o.observation_date <= op.observation_period_end_date
	GROUP BY observation_source_concept_id
	) tmp
	WHERE concept_id != 0;
}