SELECT concept.*
FROM @vocabulary_database_schema.concept
where concept.concept_id in (@conceptIds);
