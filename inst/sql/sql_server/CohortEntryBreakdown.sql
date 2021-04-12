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
	concept_count,
	subject_count
{@store} ? {
INTO @store_table
} : {
	,concept_name
}
FROM (
	SELECT '@domain_table' AS domain_table,
	  '@domain_concept_id' AS domain_field,
	  @domain_concept_id AS concept_id,
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
	
	{@use_source_concept_id} ? {
	UNION
	
	SELECT '@domain_table' AS domain_table,
	  '@domain_source_concept_id' AS domain_field,
	  @domain_source_concept_id AS concept_id,
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
	}
	
	) concept_counts
INNER JOIN @vocabulary_database_schema.concept
	ON concept_counts.concept_id = concept.concept_id;
