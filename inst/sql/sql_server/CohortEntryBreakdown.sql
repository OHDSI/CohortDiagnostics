{DEFAULT @store = FALSE}
{DEFAULT @store_table = #breakdown}
{DEFAULT @use_source_concept_id = FALSE}

{@store} ? {
IF OBJECT_ID('tempdb..@store_table', 'U') IS NOT NULL
  DROP TABLE @store_table;
}

SELECT domain_table,
  domain_field,
  concept.concept_id,
  cohort_start_year,
	sum(concept_count) as concept_count,
	max(subject_count) as subject_count
{@store} ? {
INTO @store_table
} : {
	,concept_name
}
FROM (
	-- Overall breakdown: one row per concept across the whole cohort.
	-- subject_count here is a true COUNT(DISTINCT subject_id) over all years,
	-- which cannot be reconstructed by summing the per-year rows below.
	SELECT '@domain_table' AS domain_table,
	  '@domain_concept_id' AS domain_field,
	  @domain_concept_id AS concept_id,
	  -1 AS cohort_start_year,
		COUNT(*) AS concept_count,
		COUNT(distinct subject_id) AS subject_count
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.@domain_table
		ON subject_id = person_id
			AND cohort_start_date = @domain_start_date
	INNER JOIN @concept_set_table
		ON @domain_concept_id = concept_id
	WHERE cohort_definition_id = @cohort_id
		AND codeset_id IN (@primary_codeset_ids)
	GROUP BY @domain_concept_id

	UNION

	-- Per cohort start year, for the trend view. subject_count is distinct
	-- within the year, so these rows are detail only and are not summed to
	-- produce the overall figures above.
	SELECT '@domain_table' AS domain_table,
	  '@domain_concept_id' AS domain_field,
	  @domain_concept_id AS concept_id,
	  YEAR(cohort_start_date) AS cohort_start_year,
		COUNT(*) AS concept_count,
		COUNT(distinct subject_id) AS subject_count
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.@domain_table
		ON subject_id = person_id
			AND cohort_start_date = @domain_start_date
	INNER JOIN @concept_set_table
		ON @domain_concept_id = concept_id
	WHERE cohort_definition_id = @cohort_id
		AND codeset_id IN (@primary_codeset_ids)
	GROUP BY @domain_concept_id, YEAR(cohort_start_date)
	
	{@use_source_concept_id} ? {
	UNION
	
	SELECT '@domain_table' AS domain_table,
	  '@domain_source_concept_id' AS domain_field,
	  @domain_source_concept_id AS concept_id,
	  -1 AS cohort_start_year,
		COUNT(*) AS concept_count,
		COUNT(distinct subject_id) AS subject_count
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.@domain_table
		ON subject_id = person_id
			AND cohort_start_date = @domain_start_date
	INNER JOIN @concept_set_table
		ON @domain_concept_id = concept_id
	WHERE cohort_definition_id = @cohort_id
		AND codeset_id IN (@primary_codeset_ids)
	GROUP BY @domain_source_concept_id
	
	UNION
	
	SELECT '@domain_table' AS domain_table,
	  '@domain_source_concept_id' AS domain_field,
	  @domain_source_concept_id AS concept_id,
	  YEAR(cohort_start_date) AS cohort_start_year,
		COUNT(*) AS concept_count,
		COUNT(distinct subject_id) AS subject_count
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.@domain_table
		ON subject_id = person_id
			AND cohort_start_date = @domain_start_date
	INNER JOIN @concept_set_table
		ON @domain_concept_id = concept_id
	WHERE cohort_definition_id = @cohort_id
		AND codeset_id IN (@primary_codeset_ids)
	GROUP BY @domain_source_concept_id, YEAR(cohort_start_date)
	
	UNION
	
	SELECT '@domain_table' AS domain_table,
	  '@domain_source_concept_id' AS domain_field,
	  @domain_source_concept_id AS concept_id,
	  -1 AS cohort_start_year,
		COUNT(*) AS concept_count,
		COUNT(distinct subject_id) AS subject_count
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.@domain_table
		ON subject_id = person_id
			AND cohort_start_date = @domain_start_date
	INNER JOIN @concept_set_table
		ON @domain_source_concept_id = concept_id
	WHERE cohort_definition_id = @cohort_id
		AND codeset_id IN (@primary_codeset_ids)
	GROUP BY @domain_source_concept_id
	
	UNION
	
	SELECT '@domain_table' AS domain_table,
	  '@domain_source_concept_id' AS domain_field,
	  @domain_source_concept_id AS concept_id,
	  YEAR(cohort_start_date) AS cohort_start_year,
		COUNT(*) AS concept_count,
		COUNT(distinct subject_id) AS subject_count
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.@domain_table
		ON subject_id = person_id
			AND cohort_start_date = @domain_start_date
	INNER JOIN @concept_set_table
		ON @domain_source_concept_id = concept_id
	WHERE cohort_definition_id = @cohort_id
		AND codeset_id IN (@primary_codeset_ids)
	GROUP BY @domain_source_concept_id, YEAR(cohort_start_date)
	}
	
	) concept_counts
INNER JOIN @vocabulary_database_schema.concept
	ON concept_counts.concept_id = concept.concept_id

GROUP BY domain_table, domain_field, concept.concept_id, cohort_start_year;
