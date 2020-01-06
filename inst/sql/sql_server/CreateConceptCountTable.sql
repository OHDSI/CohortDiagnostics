IF OBJECT_ID('@work_database_schema.@concept_counts_table', 'U') IS NOT NULL
	DROP TABLE @work_database_schema.@concept_counts_table;

SELECT concept_id,
	concept_count
INTO @work_database_schema.@concept_counts_table
FROM (
	SELECT condition_concept_id AS concept_id,
		COUNT_BIG(condition_concept_id) AS concept_count
	FROM @cdm_database_schema.condition_occurrence
	GROUP BY condition_concept_id
	
	UNION ALL
	
	SELECT condition_source_concept_id AS concept_id,
		COUNT_BIG(condition_source_concept_id) AS concept_count
	FROM @cdm_database_schema.condition_occurrence
	GROUP BY condition_source_concept_id
	
	UNION ALL
	
	SELECT drug_concept_id AS concept_id,
		COUNT_BIG(drug_concept_id) AS concept_count
	FROM @cdm_database_schema.drug_exposure
	GROUP BY drug_concept_id
	
	UNION ALL
	
	SELECT drug_source_concept_id AS concept_id,
		COUNT_BIG(drug_source_concept_id) AS concept_count
	FROM @cdm_database_schema.drug_exposure
	GROUP BY drug_source_concept_id
	
	UNION ALL
	
	SELECT procedure_concept_id AS concept_id,
		COUNT_BIG(procedure_concept_id) AS concept_count
	FROM @cdm_database_schema.procedure_occurrence
	GROUP BY procedure_concept_id
	
	UNION ALL
	
	SELECT procedure_source_concept_id AS concept_id,
		COUNT_BIG(procedure_source_concept_id) AS concept_count
	FROM @cdm_database_schema.procedure_occurrence
	GROUP BY procedure_source_concept_id
	
	UNION ALL
	
	SELECT measurement_concept_id AS concept_id,
		COUNT_BIG(measurement_concept_id) AS concept_count
	FROM @cdm_database_schema.measurement
	GROUP BY measurement_concept_id
	
	UNION ALL
	
	SELECT measurement_source_concept_id AS concept_id,
		COUNT_BIG(measurement_source_concept_id) AS concept_count
	FROM @cdm_database_schema.measurement
	GROUP BY measurement_source_concept_id
	
	UNION ALL
	
	SELECT observation_concept_id AS concept_id,
		COUNT_BIG(observation_concept_id) AS concept_count
	FROM @cdm_database_schema.observation
	GROUP BY observation_concept_id
	
	UNION ALL
	
	SELECT observation_source_concept_id AS concept_id,
		COUNT_BIG(observation_source_concept_id) AS concept_count
	FROM @cdm_database_schema.observation
	GROUP BY observation_source_concept_id
	) tmp;
