{DEFAULT @store = FALSE}
{DEFAULT @store_table = #breakdown}

IF OBJECT_ID('tempdb..#concepts_interest', 'U') IS NOT NULL
  DROP TABLE #concepts_interest;

WITH conceptIds AS (
		SELECT DISTINCT concept_id
		FROM @concept_set_table
		WHERE cohort_definition_id = @cohort_id
			AND codeset_id IN (@primary_codeset_ids)
		),
	descendantConcepts AS (
		SELECT DISTINCT ca.descendant_concept_id concept_id
		FROM @cdm_database_schema.concept_ancestor ca
		INNER JOIN conceptIds AS ca.ancestor_concept_id = conceptIds.concept_id
		),
	mappedConcepts AS (
		SELECT DISTINCT cr.concept_id_2 concept_id
		FROM @cdm_database_schema.concept_relationship cr
		INNER JOIN descendantConcepts c ON c.concept_id = cr.concept_id_1
		WHERE cr.INVALID_REASON IS NULL
		
		UNION
		
		SELECT DISTINCT cr.concept_id_1 concept_id
		FROM @cdm_database_schema.concept_relationship cr
		INNER JOIN descendantConcepts c ON c.concept_id = cr.concept_id_2
		WHERE cr.INVALID_REASON IS NULL
		),
	conceptIdsOfInterest AS (
		SELECT concept_id
		FROM conceptIds
		
		UNION
		
		SELECT concept_id
		FROM descendantConcepts
		
		UNION
		
		SELECT concept_id
		FROM mappedConcepts
		)
	SELECT concept_id
	INTO #concepts_interest
	FROM conceptIdsOfInterest;
	



{@store} ? {
IF OBJECT_ID('tempdb..@store_table', 'U') IS NOT NULL
  DROP TABLE @store_table;
}	
SELECT concept.concept_id,
	concept_count.concept_count,
	concept_count.subject_count
{@store} ? {
INTO @store_table
} : {
	,concept_name
}
FROM (
	SELECT @domain_concept_id AS concept_id,
		COUNT(*) AS concept_count,
		COUNT(distinct subject_id) AS subject_count
	FROM @cohort_database_schema.@cohort_table
	INNER JOIN @cdm_database_schema.@domain_table
		ON subject_id = person_id
			AND cohort_start_date = @domain_start_date
	INNER JOIN #concepts_interest
		ON (@domain_concept_id = concept_id or
		@domain_source_concept_id = concept_id)
	GROUP BY @domain_concept_id
	) concept_counts
INNER JOIN @cdm_database_schema.concept
	ON concept_counts.concept_id = concept.concept_id;


IF OBJECT_ID('tempdb..#concepts_interest', 'U') IS NOT NULL
	DROP TABLE #concepts_interest;