DROP TABLE IF EXISTS #starting_concepts;
DROP TABLE IF EXISTS #concept_synonyms;
DROP TABLE IF EXISTS #search_strings;
DROP TABLE IF EXISTS #search_str_top1000;
DROP TABLE IF EXISTS #search_long_string;

-- For each codeset, find directly included concept and source concepts that map to those
SELECT DISTINCT codeset_id,
	concept.concept_id,
	concept.concept_name
INTO #starting_concepts
FROM (
	SELECT codeset_id,
		concept_id
	FROM @resolved_concept_sets
	
	UNION
	
	SELECT codeset_id,
		concept_id_1
	FROM @resolved_concept_sets codesets
	INNER JOIN @vocabulary_database_schema.concept_relationship ON codesets.concept_id = concept_id_2
		AND concept_relationship.relationship_id = 'Maps to'
		AND concept_relationship.invalid_reasON IS NULL
	) all_concepts
INNER JOIN @vocabulary_database_schema.concept ON all_concepts.concept_id = concept.concept_id;


-- For each codeset, concept id find synonym concept names
SELECT sc1.codeset_id,
	cs1.concept_id,
	cs1.concept_synonym_name AS concept_name
INTO #concept_synonyms
FROM #starting_concepts sc1
INNER JOIN @vocabulary_database_schema.concept_synonym cs1 ON sc1.concept_id = cs1.concept_id
WHERE concept_synonym_name IS NOT NULL;


-- Create list of search strings (from concept names and synonyms), discarding those short than 5 and longer than 50 characters
-- fonts changed to lower case
SELECT DISTINCT codeset_id,
  concept_name
INTO #search_strings
FROM (
	SELECT codeset_id,
	  concept_name
	FROM #starting_concepts
	
	UNION ALL
	
	SELECT codeset_id,
	  concept_name
	FROM #concept_synonyms
	) tmp;


DROP TABLE IF EXISTS #orphan_concept_table;
CREATE TABLE #orphan_concept_table (codeset_id INT, concept_id INT);
			
DROP TABLE IF EXISTS #concept_synonyms;