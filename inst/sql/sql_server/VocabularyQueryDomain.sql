SELECT distinct domain.*
  FROM @vocabulary_database_schema.concept
INNER JOIN @vocabulary_database_schema.domain
ON concept.domain_id = domain.domain_id
WHERE concept.concept_id in (@conceptIds);
