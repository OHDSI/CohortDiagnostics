SELECT distinct vocabulary.*
  FROM @vocabulary_database_schema.concept
INNER JOIN @vocabulary_database_schema.vocabulary
ON concept.vocabulary_id = vocabulary.vocabulary_id
where concept.concept_id in (@conceptIds);
