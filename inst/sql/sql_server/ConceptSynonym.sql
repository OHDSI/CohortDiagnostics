SELECT distinct concept_synonym.*
  FROM @vocabulary_database_schema.concept_synonym
WHERE concept_synonym.concept_id in (@conceptIds);
