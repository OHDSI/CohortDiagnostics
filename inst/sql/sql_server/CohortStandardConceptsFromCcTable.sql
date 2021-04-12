{DEFAULT @concept_counts_database_schema = scratch.dbo}
{DEFAULT @concept_counts_table = concept_counts}
{DEFAULT @concept_counts_table_is_temp = FALSE}
{DEFAULT @cdm_database_schema = cdm_optum_extended_dod_v1027.dbo}

SELECT codeset_id AS concept_set_id,
	standard_concept.concept_id,
	standard_concept.concept_name,
	concept_counts.concept_count,
	concept_counts.concept_subjects
FROM #Codesets codesets

{@concept_counts_table_is_temp} ? {		
INNER JOIN @concept_counts_table concept_counts
} : {
INNER JOIN @concept_counts_database_schema.@concept_counts_table concept_counts
}
	ON concept_counts.concept_id = codesets.concept_id
INNER JOIN @vocabulary_database_schema.concept standard_concept
	ON standard_concept.concept_id = codesets.concept_id
WHERE standard_concept.standard_concept = 'S';
