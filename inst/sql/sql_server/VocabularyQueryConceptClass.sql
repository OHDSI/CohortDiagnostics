SELECT distinct concept_class.*
  FROM @vocabulary_database_schema.concept
INNER JOIN @vocabulary_database_schema.concept_class
ON concept.concept_class_id = concept_class.concept_class_id
where concept.concept_id in (@conceptIds)
