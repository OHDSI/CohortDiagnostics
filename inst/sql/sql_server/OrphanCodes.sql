DROP TABLE IF EXISTS #starting_concepts;
DROP TABLE IF EXISTS #concept_synonyms;
DROP TABLE IF EXISTS #search_strings;
DROP TABLE IF EXISTS #search_str_top1000;
DROP TABLE IF EXISTS #search_string_subset;
DROP TABLE IF EXISTS #orphan_concept_table;

-- Find directly included concept and source concepts that map to those
SELECT DISTINCT codeset_id,
  concept.concept_id,
	concept_name
INTO #starting_concepts
FROM (
	SELECT codeset_id, concept_id
	FROM @resolved_concept_sets
	
	UNION
	
	SELECT codeset_id, concept_id_1
	FROM @resolved_concept_sets codesets
	INNER JOIN @vocabulary_database_schema.concept_relationship
		ON codesets.concept_id = concept_id_2
			AND concept_relationship.relationship_id = 'Maps to'
			AND concept_relationship.invalid_reasON IS NULL
	) all_concepts
INNER JOIN @vocabulary_database_schema.concept
	ON all_concepts.concept_id = concept.concept_id;

-- Find synonyms
SELECT sc1.codeset_id,
  cs1.concept_id,
	cs1.concept_synonym_name AS concept_name
INTO #concept_synonyms
FROM #starting_concepts sc1
INNER JOIN @vocabulary_database_schema.concept_synonym cs1
	ON sc1.concept_id = cs1.concept_id
WHERE concept_synonym_name IS NOT NULL;

-- Create list of search strings from concept names and synonyms, discarding those short than 5 and longer than 50 characters
SELECT DISTINCT codeset_id,
  concept_name,
	concept_name_length,
	concept_name_terms
INTO #search_strings
FROM (
	SELECT codeset_id,
	  LOWER(concept_name) AS concept_name,
		LEN(concept_name) AS concept_name_length,
		LEN(concept_name) - LEN(REPLACE(concept_name, ' ', '')) + 1 AS concept_name_terms
	FROM #starting_concepts
	WHERE len(concept_name) > 5
		AND len(concept_name) < 50
	
	UNION ALL
	
	SELECT codeset_id,
	  LOWER(concept_name) AS concept_name,
		LEN(concept_name) AS concept_name_length,
		LEN(concept_name) - LEN(REPLACE(concept_name, ' ', '')) + 1 AS concept_name_terms
	FROM #concept_synonyms
	WHERE len(concept_name) > 5
		AND len(concept_name) < 50
	) tmp;


-- Order search terms by length (words and characters), take top 1000 per codeset_id
SELECT codeset_id,
  concept_name,
	concept_name_length,
	concept_name_terms
INTO #search_str_top1000
FROM (
	SELECT codeset_id,
	  concept_name,
		concept_name_length,
		concept_name_terms,
		row_number() OVER (PARTITION BY codeset_id
			ORDER BY concept_name_terms ASC,
				concept_name_length ASC
			) AS rn1
	FROM #search_strings
	) t1
WHERE rn1 < 1000;

-- If search string is substring of another search string, discard longer string
WITH longer_string
AS (
	SELECT DISTINCT ss1.codeset_id,
		ss1.concept_name
	FROM #search_str_top1000 ss1
	INNER JOIN #search_str_top1000 ss2 ON 
	  ss2.concept_name_length < ss1.concept_name_length
		AND ss1.codeset_id = ss2.codeset_id
		AND ss1.concept_name LIKE CONCAT (
			'%',
			ss2.concept_name,
			'%'
			)
	)
SELECT DISTINCT ss1.*
INTO #search_string_subset
FROM #search_str_top1000 ss1
LEFT JOIN longer_string ls ON ss1.codeset_id = ls.codeset_id
	AND ss1.concept_name = ls.concept_name
WHERE ls.codeset_id IS NULL;

-- Create recommended list: concepts containing search string but not mapping to start set
SELECT DISTINCT ss1.codeset_id,
  c1.concept_id
INTO #orphan_concept_table
FROM @vocabulary_database_schema.concept c1
INNER JOIN #search_string_subset ss1
	ON LOWER(c1.concept_name) LIKE CONCAT (
			'%',
			ss1.concept_name,
			'%'
			)
LEFT JOIN #starting_concepts sc
ON c1.concept_id = sc.concept_id
AND ss1.codeset_id = sc.codeset_id
WHERE sc.concept_id IS NULL;
			
			
DROP TABLE IF EXISTS #starting_concepts;
DROP TABLE IF EXISTS #concept_synonyms;
DROP TABLE IF EXISTS #search_strings;
DROP TABLE IF EXISTS #search_str_top1000;
DROP TABLE IF EXISTS #search_string_subset;