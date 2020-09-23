{DEFAULT @by_month = false}
{DEFAULT @use_source_values = false}

IF OBJECT_ID('tempdb..#all_concept_ids', 'U') IS NOT NULL
  DROP TABLE #all_concept_ids;
  
SELECT DISTINCT concept_id
INTO #all_concept_ids
FROM @instantiated_concept_sets;

IF OBJECT_ID('tempdb..@include_source_concept_table', 'U') IS NOT NULL
  DROP TABLE @include_source_concept_table;

SELECT codeset_id AS concept_set_id,
	concept_sets.concept_id,
	source_concept_id,
{@by_month} ? {
	event_year,
	event_month,
}
	concept_subjects,
	concept_count
INTO @include_source_concept_table
FROM @instantiated_concept_sets concept_sets
INNER JOIN (

-- condition_occurrence
	SELECT condition_concept_id AS concept_id,
		condition_source_concept_id AS source_concept_id,
{@by_month} ? {		
		YEAR(condition_start_date) AS event_year,
		MONTH(condition_start_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @cdm_database_schema.condition_occurrence
	INNER JOIN @cdm_database_schema.observation_period
		ON condition_occurrence.person_id = observation_period.person_id
			AND condition_start_date >= observation_period_start_date
			AND condition_start_date <= observation_period_end_date
	WHERE condition_concept_id IN (SELECT concept_id FROM #all_concept_ids)
	GROUP BY condition_concept_id,
{@by_month} ? {		
		YEAR(condition_start_date),
		MONTH(condition_start_date),	
}
		condition_source_concept_id
	
	UNION ALL
	
-- drug_exposure
	SELECT drug_concept_id AS concept_id,
		drug_source_concept_id AS source_concept_id,
{@by_month} ? {		
		YEAR(drug_exposure_start_date) AS event_year,
		MONTH(drug_exposure_start_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @cdm_database_schema.drug_exposure
	INNER JOIN @cdm_database_schema.observation_period
		ON drug_exposure.person_id = observation_period.person_id
			AND drug_exposure_start_date >= observation_period_start_date
			AND drug_exposure_start_date <= observation_period_end_date
	WHERE drug_concept_id IN (SELECT concept_id FROM #all_concept_ids)
	GROUP BY drug_concept_id,
{@by_month} ? {		
		YEAR(drug_exposure_start_date),
		MONTH(drug_exposure_start_date),	
}
		drug_source_concept_id
		
	UNION ALL
		
-- procedure_occurrence
	SELECT procedure_concept_id AS concept_id,
		procedure_source_concept_id AS source_concept_id,
{@by_month} ? {	
		YEAR(procedure_date) AS event_year,
		MONTH(procedure_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @cdm_database_schema.procedure_occurrence
	INNER JOIN @cdm_database_schema.observation_period
		ON procedure_occurrence.person_id = observation_period.person_id
			AND procedure_date >= observation_period_start_date
			AND procedure_date <= observation_period_end_date
	GROUP BY procedure_concept_id,
{@by_month} ? {	
		YEAR(procedure_date),
		MONTH(procedure_date),	
}
		procedure_source_concept_id
	
	UNION ALL
	
-- measurement
	SELECT measurement_concept_id AS concept_id,
		measurement_source_concept_id AS source_concept_id,
{@by_month} ? {	
		YEAR(measurement_date) AS event_year,
		MONTH(measurement_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @cdm_database_schema.measurement
	INNER JOIN @cdm_database_schema.observation_period
		ON measurement.person_id = observation_period.person_id
			AND measurement_date >= observation_period_start_date
			AND measurement_date <= observation_period_end_date
	GROUP BY measurement_concept_id,
{@by_month} ? {	
		YEAR(measurement_date),
		MONTH(measurement_date),	
}
		measurement_source_concept_id
	
	UNION ALL
	
-- observation
	SELECT observation_concept_id AS concept_id,
		observation_source_concept_id AS source_concept_id,
{@by_month} ? {	
		YEAR(observation_date) AS event_year,
		MONTH(observation_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @cdm_database_schema.observation
	INNER JOIN @cdm_database_schema.observation_period
		ON observation.person_id = observation_period.person_id
			AND observation_date >= observation_period_start_date
			AND observation_date <= observation_period_end_date
	GROUP BY observation_concept_id,
{@by_month} ? {			
		YEAR(observation_date),
		MONTH(observation_date),
}
		observation_source_concept_id
		
	UNION ALL
	
-- visit_occurrence
	SELECT visit_concept_id AS concept_id,
		visit_source_concept_id AS source_concept_id,
{@by_month} ? {			
		YEAR(visit_start_date) AS event_year,
		MONTH(visit_start_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @cdm_database_schema.visit_occurrence
	INNER JOIN @cdm_database_schema.observation_period
		ON visit_occurrence.person_id = observation_period.person_id
			AND visit_start_date >= observation_period_start_date
			AND visit_start_date <= observation_period_end_date
	GROUP BY visit_concept_id,
{@by_month} ? {			
		YEAR(visit_start_date),
		MONTH(visit_start_date),
}
		visit_source_concept_id
		
	) concept_counts
	ON concept_sets.concept_id = concept_counts.concept_id;

TRUNCATE TABLE #all_concept_ids;

DROP TABLE #all_concept_ids;
