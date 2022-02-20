IF OBJECT_ID('tempdb..#starting_concepts', 'U') IS NOT NULL
  DROP TABLE #starting_concepts;
  
IF OBJECT_ID('tempdb..#concept_synonyms', 'U') IS NOT NULL
  DROP TABLE #concept_synonyms;
  
IF OBJECT_ID('tempdb..#search_strings', 'U') IS NOT NULL
  DROP TABLE #search_strings;
  
IF OBJECT_ID('tempdb..#search_str_top1000', 'U') IS NOT NULL
  DROP TABLE #search_str_top1000;
  
IF OBJECT_ID('tempdb..#search_string_subset', 'U') IS NOT NULL
  DROP TABLE #search_string_subset;

IF OBJECT_ID('tempdb..#orphan_concept_table', 'U') IS NOT NULL
  DROP TABLE #orphan_concept_table;

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
SELECT codeset_id,
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
	
	UNION
	
	SELECT codeset_id,
	  LOWER(concept_name) AS concept_name,
		LEN(concept_name) AS concept_name_length,
		LEN(concept_name) - LEN(REPLACE(concept_name, ' ', '')) + 1 AS concept_name_terms
	FROM #concept_synonyms
	WHERE len(concept_name) > 5
		AND len(concept_name) < 50
	) tmp;


-- Order search terms by length (words and characters), take top 1000
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
SELECT ss1.*
INTO #search_string_subset
FROM #search_str_top1000 ss1
WHERE ss1.concept_name NOT IN (
    SELECT ss1.concept_name
    FROM #search_str_top1000 ss1
        INNER JOIN #search_str_top1000 ss2
            ON ss2.concept_name_length < ss1.concept_name_length
                   AND ss1.codeset_id = ss2.codeset_id
                   AND ss1.concept_name LIKE CONCAT ('%', ss2.concept_name, '%')
    );

-- Create recommended list: concepts containing search string but not mapping to start set
SELECT DISTINCT ss1.codeset_id,
  c1.concept_id
INTO #orphan_concept_table
FROM (
	SELECT c1.concept_id,
		c1.concept_name
	FROM @vocabulary_database_schema.concept c1
	LEFT JOIN (SELECT DISTINCT concept_id FROM #starting_concepts) sc1
		ON c1.concept_id = sc1.concept_id
	WHERE sc1.concept_id IS NULL
	) c1
INNER JOIN #search_string_subset ss1
	ON LOWER(c1.concept_name) LIKE CONCAT (
			'%',
			ss1.concept_name,
			'%'
			);