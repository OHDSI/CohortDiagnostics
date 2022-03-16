DROP TABLE IF EXISTS #starting_concepts;
DROP TABLE IF EXISTS #concept_synonyms;
DROP TABLE IF EXISTS #search_strings;
DROP TABLE IF EXISTS #search_str_top1000;
DROP TABLE IF EXISTS #search_long_string;
DROP TABLE IF EXISTS #string_not_long;
DROP TABLE IF EXISTS #string_not_long_id;
DROP TABLE IF EXISTS #string_search_results;
DROP TABLE IF EXISTS #string_search_results2;
DROP TABLE IF EXISTS #orphan_concept_table;

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
  concept_name search_string,
	search_string_length,
	search_string_terms
INTO #search_strings
FROM (
	SELECT codeset_id,
	  LOWER(concept_name) AS concept_name,
		LEN(concept_name) AS search_string_length,
		LEN(concept_name) - LEN(REPLACE(concept_name, ' ', '')) + 1 AS search_string_terms
	FROM #starting_concepts
	WHERE len(concept_name) > 5
		AND len(concept_name) < 50
	
	UNION ALL
	
	SELECT codeset_id,
	  LOWER(concept_name) AS concept_name,
		LEN(concept_name) AS search_string_length,
		LEN(concept_name) - LEN(REPLACE(concept_name, ' ', '')) + 1 AS search_string_terms
	FROM #concept_synonyms
	WHERE len(concept_name) > 5
		AND len(concept_name) < 50
	) tmp;


-- Order search terms by length (words and characters), take top 1000 per codeset_id
SELECT DISTINCT codeset_id,
	search_string,
	search_string_length,
	search_string_terms
INTO #search_str_top1000
FROM (
	SELECT codeset_id,
		search_string,
		search_string_length,
		search_string_terms,
		ROW_NUMBER() OVER (
			PARTITION BY codeset_id ORDER BY search_string_terms ASC,
				search_string_length ASC
			) AS rn1
	FROM #search_strings
	) t1
WHERE rn1 < 1000;


-- Find long strings, i.e. search string in a codeset id that is a substring of another search string in same codeset id
SELECT DISTINCT ss1.codeset_id,
	ss1.search_string search_string_long
INTO #search_long_string
FROM #search_str_top1000 ss1
INNER JOIN #search_str_top1000 ss2 ON ss2.search_string_length < ss1.search_string_length
	AND ss1.codeset_id = ss2.codeset_id
	AND ss1.search_string LIKE CONCAT (
		'%',
		ss2.search_string,
		'%'
		);
	
-- Discard longer string from search_string
SELECT DISTINCT ss1.codeset_id,
	ss1.search_string string_not_long
INTO #string_not_long
FROM #search_str_top1000 ss1
LEFT JOIN #search_long_string ls ON ss1.codeset_id = ls.codeset_id
	AND ss1.search_string = ls.search_string_long
WHERE ls.codeset_id IS NULL;


-- add unique id for each search_string
SELECT ss.string_not_long,
	ROW_NUMBER() OVER (
		ORDER BY ss.string_not_long
		) search_string_id
INTO #string_not_long_id
FROM (
	SELECT DISTINCT string_not_long
	FROM #string_not_long
	) ss;

 
-- find concept id LIKE a search string - these are concept id that are returned based on string search
-- this takes the most time
SELECT DISTINCT ss1.search_string_id,
	c1.concept_id
INTO #string_search_results
FROM @vocabulary_database_schema.concept c1
INNER JOIN #string_not_long_id ss1 ON LOWER(c1.concept_name) LIKE CONCAT (
		'%',
		ss1.string_not_long,
		'%'
		)
	AND c1.invalid_reason IS NULL;


-- add back codeset_id 
SELECT snls.codeset_id,
	ssr.concept_id
INTO #string_search_results2
FROM #string_search_results ssr
INNER JOIN #string_not_long_id snlsid ON ssr.search_string_id = snlsid.search_string_id
INNER JOIN #string_not_long snls ON snlsid.string_not_long = snls.string_not_long;

	
-- Create recommended list: concepts containing search string but not mapping to start set
SELECT DISTINCT sc.codeset_id,
	c1.concept_id,
	c1.concept_name
INTO #orphan_concept_table
FROM #string_search_results2 c1
LEFT JOIN #starting_concepts sc ON c1.concept_id = sc.concept_id
	AND c1.codeset_id = sc.codeset_id
WHERE sc.concept_id IS NULL;

			
			