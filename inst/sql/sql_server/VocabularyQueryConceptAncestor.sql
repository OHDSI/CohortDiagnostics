SELECT distinct concept_ancestor.*
  FROM @vocabulary_database_schema.concept_ancestor
where concept_ancestor.ancestor_concept_id in (@conceptIds) or
concept_ancestor.descendant_concept_id  in (@conceptIds);
