{DEFAULT @concept_counts_database_schema = scratch.dbo}
{DEFAULT @concept_counts_table = concept_counts}
{DEFAULT @concept_counts_table_is_temp = FALSE}
{DEFAULT @cdm_database_schema = cdm_optum_extended_dod_v1027.dbo}

SELECT codeset_id AS concept_set_id,
	standard_concept.concept_id,
	standard_concept.concept_name,
	source_concept.concept_code,
	source_concept.vocabulary_id AS source_vocabulary_id,
	source_concept.concept_id AS source_concept_id,
	source_concept.concept_name AS source_concept_name,
	source_concept.concept_count,
	source_concept.concept_subjects
FROM #Codesets codesets
INNER JOIN @vocabulary_database_schema.concept standard_concept
	ON standard_concept.concept_id = codesets.concept_id
INNER JOIN (
  SELECT concept_id_2 AS standard_concept_id,
    concept.concept_id,
    concept.concept_name,
    concept.concept_code,
    concept.vocabulary_id,
	concept_count,
	concept_subjects
  FROM @vocabulary_database_schema.concept_relationship
  INNER JOIN @vocabulary_database_schema.concept
  	ON concept.concept_id = concept_id_1
{@concept_counts_table_is_temp} ? {		
  INNER JOIN @concept_counts_table concept_counts
} : {
  INNER JOIN @concept_counts_database_schema.@concept_counts_table concept_counts
}
	 ON concept_counts.concept_id = concept.concept_id  	
  WHERE relationship_id = 'Maps to'
    AND (standard_concept IS NULL OR standard_concept != 'S')
 ) source_concept
 	ON source_concept.standard_concept_id = standard_concept.concept_id
WHERE standard_concept.standard_concept = 'S';
