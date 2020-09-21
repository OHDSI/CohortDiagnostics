{DEFAULT @by_month = false}
{DEFAULT @use_source_values = false}

IF OBJECT_ID('tempdb..@include_source_concept_table', 'U') IS NOT NULL
  DROP TABLE @include_source_concept_table;

SELECT codeset_id AS concept_set_id,
	concept.concept_id,
	source_concept_id,
{@by_month} ? {
	event_year,
	event_month,
}
	concept_subjects,
	concept_count
INTO @include_source_concept_table
FROM (

-- condition_occurrence
	SELECT codeset_id,
		concept_id,
		condition_source_concept_id AS source_concept_id,
{@by_month} ? {		
		YEAR(condition_start_date) AS event_year,
		MONTH(condition_start_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @instantiated_concept_sets
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON condition_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON condition_occurrence.person_id = observation_period.person_id
			AND condition_start_date >= observation_period_start_date
			AND condition_start_date <= observation_period_end_date
	GROUP BY codeset_id,
		condition_source_concept_id,
{@by_month} ? {		
		YEAR(condition_start_date),
		MONTH(condition_start_date),	
}
		concept_id
	
	UNION ALL
	
-- drug_exposure
	SELECT codeset_id,
		concept_id,
		drug_source_concept_id AS source_concept_id,
{@by_month} ? {		
		YEAR(drug_exposure_start_date) AS event_year,
		MONTH(drug_exposure_start_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @instantiated_concept_sets
	INNER JOIN @cdm_database_schema.drug_exposure
		ON drug_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON drug_exposure.person_id = observation_period.person_id
			AND drug_exposure_start_date >= observation_period_start_date
			AND drug_exposure_start_date <= observation_period_end_date
	GROUP BY codeset_id,
		drug_source_concept_id,
{@by_month} ? {		
		YEAR(drug_exposure_start_date),
		MONTH(drug_exposure_start_date),	
}
		concept_id
	
	UNION ALL
	
-- procedure_occurrence
	SELECT codeset_id,
		concept_id,
		procedure_source_concept_id AS source_concept_id,
{@by_month} ? {	
		YEAR(procedure_date) AS event_year,
		MONTH(procedure_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @instantiated_concept_sets
	INNER JOIN @cdm_database_schema.procedure_occurrence
		ON procedure_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON procedure_occurrence.person_id = observation_period.person_id
			AND procedure_date >= observation_period_start_date
			AND procedure_date <= observation_period_end_date
	GROUP BY codeset_id,
		procedure_source_concept_id,
{@by_month} ? {	
		YEAR(procedure_date),
		MONTH(procedure_date),	
}
		concept_id
	
	UNION ALL
	
-- measurement
	SELECT codeset_id,
		concept_id,
		measurement_source_concept_id AS source_concept_id,
{@by_month} ? {	
		YEAR(measurement_date) AS event_year,
		MONTH(measurement_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @instantiated_concept_sets
	INNER JOIN @cdm_database_schema.measurement
		ON measurement_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON measurement.person_id = observation_period.person_id
			AND measurement_date >= observation_period_start_date
			AND measurement_date <= observation_period_end_date
	GROUP BY codeset_id,
		measurement_source_concept_id,
{@by_month} ? {	
		YEAR(measurement_date),
		MONTH(measurement_date),	
}
		concept_id
	
	UNION ALL
	
-- observation
	SELECT codeset_id,
		concept_id,
		observation_source_concept_id AS source_concept_id,
{@by_month} ? {	
		YEAR(observation_date) AS event_year,
		MONTH(observation_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @instantiated_concept_sets
	INNER JOIN @cdm_database_schema.observation
		ON observation_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON observation.person_id = observation_period.person_id
			AND observation_date >= observation_period_start_date
			AND observation_date <= observation_period_end_date
	GROUP BY codeset_id,
		observation_source_concept_id,
{@by_month} ? {			
		YEAR(observation_date),
		MONTH(observation_date),
}
		concept_id
		
	UNION ALL
	
-- visit_occurrence
	SELECT codeset_id,
		concept_id,
		visit_source_concept_id AS source_concept_id,
{@by_month} ? {			
		YEAR(visit_start_date) AS event_year,
		MONTH(visit_start_date) AS event_month,
}
		COUNT_BIG(DISTINCT observation_period.person_id) AS concept_subjects,
		COUNT_BIG(*) AS concept_count
	FROM @instantiated_concept_sets
	INNER JOIN @cdm_database_schema.visit_occurrence
		ON visit_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON visit_occurrence.person_id = observation_period.person_id
			AND visit_start_date >= observation_period_start_date
			AND visit_start_date <= observation_period_end_date
	GROUP BY codeset_id,
		visit_source_concept_id,
{@by_month} ? {			
		YEAR(visit_start_date),
		MONTH(visit_start_date),
}
		concept_id
	) person_counts
INNER JOIN @cdm_database_schema.concept
	ON person_counts.concept_id = concept.concept_id
LEFT JOIN @cdm_database_schema.concept source_concept
	ON source_concept_id = source_concept.concept_id;
