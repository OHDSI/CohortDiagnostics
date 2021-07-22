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

IF OBJECT_ID('tempdb..@orphan_concept_table', 'U') IS NOT NULL
	DROP TABLE @orphan_concept_table;

WITH instantiated_concepts (concept_id)
AS (
	SELECT DISTINCT concept_id
	FROM @instantiated_code_sets
	)
SELECT DISTINCT concept_id,
	concept_name
INTO #starting_concepts
FROM (
	SELECT c1.concept_id,
		c1.concept_name
	FROM @vocabulary_database_schema.concept c1
	INNER JOIN instantiated_concepts i
		ON c1.concept_id = i.concept_id
	
	UNION
	
	SELECT ca1.ancestor_concept_id concept_id,
		c1.concept_name
	FROM @vocabulary_database_schema.concept_ancestor ca1
	INNER JOIN instantiated_concepts i
		ON ca1.ancestor_concept_id = i.concept_id
	INNER JOIN @vocabulary_database_schema.concept c1
		ON ca1.descendant_concept_id = c1.concept_id
	
	UNION
	
	SELECT ca1.ancestor_concept_id concept_id,
		c1.concept_name
	FROM @vocabulary_database_schema.concept_ancestor ca1
	INNER JOIN instantiated_concepts i
		ON ca1.ancestor_concept_id = i.concept_id
	INNER JOIN @vocabulary_database_schema.concept_relationship cr1
		ON ca1.descendant_concept_id = cr1.concept_id_2
			AND cr1.relationship_id = 'Maps to'
			AND cr1.invalid_reasON IS NULL
	INNER JOIN @vocabulary_database_schema.concept c1
		ON ca1.descendant_concept_id = c1.concept_id
	) tmp
order by concept_id,
	concept_name;

-- Find synonyms
SELECT cs1.concept_id,
	cs1.concept_synonym_name AS concept_name
INTO #concept_synonyms
FROM (SELECT DISTINCT concept_id FROM #starting_concepts) sc1
INNER JOIN @vocabulary_database_schema.concept_synonym cs1
	ON sc1.concept_id = cs1.concept_id
WHERE concept_synonym_name IS NOT NULL;

-- Create list of search strings from concept names and synonyms, discarding those short than 5 and longer than 50 characters
SELECT DISTINCT concept_id,
	concept_name,
	concept_name_length,
	concept_name_terms
INTO #search_strings
FROM (
	SELECT concept_id,
		LOWER(concept_name) AS concept_name,
		LEN(concept_name) AS concept_name_length,
		LEN(concept_name) - LEN(REPLACE(concept_name, ' ', '')) + 1 AS concept_name_terms
	FROM #starting_concepts
	WHERE len(concept_name) > 5
		AND len(concept_name) < 50
	
	UNION ALL
	
	SELECT concept_id,
		LOWER(concept_name) AS concept_name,
		LEN(concept_name) AS concept_name_length,
		LEN(concept_name) - LEN(REPLACE(concept_name, ' ', '')) + 1 AS concept_name_terms
	FROM #concept_synonyms
	WHERE len(concept_name) > 5
		AND len(concept_name) < 50
	) tmp
order by concept_id, concept_name;

-- Order search terms by length (words and characters), take top 1000
--HINT DISTRIBUTE_ON_KEY(concept_name)
SELECT DISTINCT concept_id,
	concept_name,
	concept_name_length,
	concept_name_terms
INTO #search_str_top1000
FROM (
	SELECT concept_id,
		concept_name,
		concept_name_length,
		concept_name_terms,
		row_number() OVER (
			PARTITION BY concept_id
			ORDER BY concept_name_terms ASC,
				concept_name_length ASC
			) AS rn1
	FROM #search_strings
	) t1
WHERE rn1 < 1000
order by concept_id, concept_name;

-- Find substring of another search string
-- removed the substring removal logic in prior SQL
SELECT DISTINCT ss1.concept_id,
	ss1.concept_name
INTO #search_sub_str
FROM #search_str_top1000 ss1
INNER JOIN #search_str_top1000 ss2
	ON ss1.concept_id = ss2.concept_id
		AND ss2.concept_name_length < ss1.concept_name_length
		AND ss1.concept_name LIKE CONCAT (
			'%',
			ss2.concept_name,
			'%'
			)
order by ss1.concept_id, ss1.concept_name;


-- Create recommended list: concepts containing search string but not mapping to start set
SELECT DISTINCT ss1.concept_id,
	c1.concept_id orphan_concept_id,
INTO @orphan_concept_table
FROM @vocabulary_database_schema.concept
INNER JOIN #search_string_subset ss1
	ON LOWER(c1.concept_name) LIKE CONCAT (
			'%',
			ss1.concept_name,
			'%'
			)
WHERE ss1.concept_id != c1.concept_id
ORDER BY ss1.concept_id,
	c1.concept_id;

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
