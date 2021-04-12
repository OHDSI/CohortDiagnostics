SELECT distinct concept_relationship.*
  FROM @vocabulary_database_schema.concept_relationship
WHERE concept_relationship.concept_id_1 in (@conceptIds) or
concept_relationship.concept_id_2 in (@conceptIds);
