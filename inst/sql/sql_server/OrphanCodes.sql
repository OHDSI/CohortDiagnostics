IF OBJECT_ID('tempdb..#starting_concepts', 'U') IS NOT NULL
	DROP TABLE #starting_concepts;

IF OBJECT_ID('tempdb..#concept_synonyms', 'U') IS NOT NULL
	DROP TABLE #concept_synonyms;

IF OBJECT_ID('tempdb..#search_strings', 'U') IS NOT NULL
	DROP TABLE #search_strings;

IF OBJECT_ID('tempdb..#search_str_top1000', 'U') IS NOT NULL
	DROP TABLE #search_str_top1000;

IF OBJECT_ID('tempdb..#search_sub_str', 'U') IS NOT NULL
	DROP TABLE #search_sub_str;

IF OBJECT_ID('tempdb..#search_string_subset', 'U') IS NOT NULL
	DROP TABLE #search_string_subset;

IF OBJECT_ID('tempdb..#orphan_concept_table', 'U') IS NOT NULL
	DROP TABLE #orphan_concept_table;

SELECT DISTINCT codeset_id,
	concept_name
INTO #starting_concepts
FROM (
	SELECT DISTINCT i.codeset_id,
		c1.concept_name
	FROM @vocabulary_database_schema.concept c1
	INNER JOIN @instantiated_code_sets i
		ON c1.concept_id = i.concept_id
	
	UNION
	
	SELECT DISTINCT i.codeset_id,
		c1.concept_name
	FROM @vocabulary_database_schema.concept_ancestor ca1
	INNER JOIN @instantiated_code_sets i
		ON ca1.ancestor_concept_id = i.concept_id
	INNER JOIN @vocabulary_database_schema.concept c1
		ON ca1.descendant_concept_id = c1.concept_id
	
	UNION
	
	SELECT DISTINCT i.codeset_id,
		c1.concept_name
	FROM @vocabulary_database_schema.concept_ancestor ca1
	INNER JOIN @instantiated_code_sets i
		ON ca1.ancestor_concept_id = i.concept_id
	INNER JOIN @vocabulary_database_schema.concept_relationship cr1
		ON ca1.descendant_concept_id = cr1.concept_id_2
			AND cr1.relationship_id = 'Maps to'
			AND cr1.invalid_reasON IS NULL
	INNER JOIN @vocabulary_database_schema.concept c1
		ON ca1.descendant_concept_id = c1.concept_id
	
	UNION
	
	SELECT i.codeset_id,
		cs1.concept_synonym_name AS concept_name
	FROM @vocabulary_database_schema.concept_ancestor ca1
	INNER JOIN @instantiated_code_sets i
		ON ca1.ancestor_concept_id = i.concept_id
	INNER JOIN @vocabulary_database_schema.concept_synonym cs1
		ON ca1.descendant_concept_id = cs1.concept_id
	) tmp
ORDER BY codeset_id,
	concept_name;

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
	) tmp
ORDER BY codeset_id,
	concept_name;

-- Order search terms by length (words and characters), take top 1000
--HINT DISTRIBUTE_ON_KEY(concept_name)
SELECT DISTINCT codeset_id,
	concept_name,
	concept_name_length,
	concept_name_terms
INTO #search_str_top1000
FROM (
	SELECT codeset_id,
		concept_name,
		concept_name_length,
		concept_name_terms,
		row_number() OVER (
			PARTITION BY codeset_id ORDER BY concept_name_terms ASC,
				concept_name_length ASC
			) AS rn1
	FROM #search_strings
	) t1
WHERE rn1 < 1000
ORDER BY codeset_id,
	concept_name;

-- If search string is substring of another search string, discard longer string
WITH longerString
AS (
	SELECT DISTINCT ss1.codeset_id,
		ss1.concept_name
	FROM #search_str_top1000 ss1
	INNER JOIN #search_str_top1000 ss2
		ON ss2.concept_name_length < ss1.concept_name_length
			AND ss1.codeset_id = ss2.codeset_id
			AND ss1.concept_name LIKE CONCAT (
				'%',
				ss2.concept_name,
				'%'
				)
	)
SELECT ss.*
INTO #search_string_subset
FROM #search_str_top1000 ss
LEFT JOIN longerString ls
	ON ss.concept_name = ls.concept_name
		AND ss.codeset_id = ls.codeset_id;

-- Create recommended list: concepts containing search string but not mapping to start set
SELECT DISTINCT ss.codeset_id,
	c1.concept_id
INTO #orphan_concept_table
FROM #search_string_subset ss
INNER JOIN @vocabulary_database_schema.concept c1
	ON LOWER(c1.concept_name) = LOWER(ss.concept_name)
LEFT JOIN @instantiated_code_sets i
	ON ss.codeset_id = i.codeset_id
		AND c1.concept_id = i.concept_id
WHERE i.codeset_id IS NULL
ORDER BY ss.codeset_id,
	c1.concept_id;

-- add the orphan codes to concept id universe table
INSERT INTO @concept_id_universe
SELECT DISTINCT concept_id
FROM #orphan_concept_table;

IF OBJECT_ID('tempdb..#starting_concepts', 'U') IS NOT NULL
	DROP TABLE #starting_concepts;

IF OBJECT_ID('tempdb..#concept_synonyms', 'U') IS NOT NULL
	DROP TABLE #concept_synonyms;

IF OBJECT_ID('tempdb..#search_strings', 'U') IS NOT NULL
	DROP TABLE #search_strings;

IF OBJECT_ID('tempdb..#search_str_top1000', 'U') IS NOT NULL
	DROP TABLE #search_str_top1000;

IF OBJECT_ID('tempdb..#search_sub_str', 'U') IS NOT NULL
	DROP TABLE #search_sub_str;

IF OBJECT_ID('tempdb..#search_string_subset', 'U') IS NOT NULL
	DROP TABLE #search_string_subset;