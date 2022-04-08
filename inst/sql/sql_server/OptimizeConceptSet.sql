DROP TABLE IF EXISTS #o_not_excluded;
DROP TABLE IF EXISTS #o_not_excluded_desc;
DROP TABLE IF EXISTS #o_not_excl_non_std;
DROP TABLE IF EXISTS #o_excluded;
DROP TABLE IF EXISTS #o_excluded_desc;
DROP TABLE IF EXISTS #o_concepts_included;
DROP TABLE IF EXISTS #o_concepts_excluded;
DROP TABLE IF EXISTS #concepts_optimized;

{DEFAULT @conceptSetConceptIdsExcluded = 0 } 
{DEFAULT @conceptSetConceptIdsDescendantsExcluded = 0 } 
{DEFAULT @conceptSetConceptIdsNotExcluded = 0 } 
{DEFAULT @conceptSetConceptIdsDescendantsNotExcluded = 0 }


-- Concepts that are part of the concept set definition that are "EXCLUDED = N, DECENDANTS = Y or N"
SELECT DISTINCT concept_id,
	standard_concept,
	invalid_reason
INTO #o_not_excluded
FROM @vocabulary_database_schema.concept
WHERE concept_id IN (@conceptSetConceptIdsNotExcluded);


-- Concepts that are part of the concept set definition that are "EXCLUDED = N, DECENDANTS = Y"
SELECT DISTINCT ancestor_concept_id,
	descendant_concept_id concept_id
INTO #o_not_excluded_desc
FROM @vocabulary_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (@conceptSetConceptIdsDescendantsNotExcluded)
	AND ancestor_concept_id != descendant_concept_id;-- Exclude the selected ancestor itself	


-- Concepts that are part of the concept set definition that are "EXCLUDED = N, DECENDANTS = Y"
SELECT cr.concept_id_1 concept_id,
	cr.concept_id_2 concept_id_standard,
	allStandard.concept_id available_standard_concept_id
INTO #o_not_excl_non_std
FROM @vocabulary_database_schema.concept_relationship cr
INNER JOIN #o_not_excluded notexc ON notexc.concept_id = concept_id_1
	AND relationship_id = 'Maps to'
LEFT JOIN (
	  SELECT DISTINCT a1.concept_id 
	  FROM #o_not_excluded a1
	  WHERE ISNULL(standard_concept,'') = 'S'
	  UNION
	  SELECT DISTINCT a2.concept_id
	  FROM #o_not_excluded_desc a2
	) allStandard
	ON cr.concept_id_2 = allStandard.concept_id
		AND relationship_id = 'Maps to'
WHERE ISNULL(standard_concept,'') = ''
	AND ISNULL(cr.invalid_reason,'') = '';


-- Concepts that are part of the concept set definition that are "EXCLUDED = Y, DECENDANTS = Y or N"
SELECT DISTINCT concept_id,
	standard_concept,
	invalid_reason
INTO #o_excluded
FROM @vocabulary_database_schema.concept
WHERE concept_id IN (@conceptSetConceptIdsExcluded);


-- Concepts that are part of the concept set definition that are "EXCLUDED = Y, DECENDANTS = Y"
SELECT DISTINCT ancestor_concept_id,
	descendant_concept_id concept_id
INTO #o_excluded_desc
FROM @vocabulary_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (@conceptSetConceptIdsDescendantsExcluded)
	AND ancestor_concept_id != descendant_concept_id;


-- Exclude the selected ancestor itself
SELECT a.concept_id original_concept_id,
	a.invalid_reason,
	c1.concept_name original_concept_name,
	b.ancestor_concept_id,
	c3.concept_name ancestor_concept_name,
	d.available_standard_concept_id mapped_concept_id,
	c4.concept_name mapped_concept_name,
	ISNULL(b.ancestor_concept_id, d.available_standard_concept_id) subsumed_concept_id,
	ISNULL(c3.concept_name, c4.concept_name) subsumed_concept_name
INTO #o_concepts_included
FROM #o_not_excluded a
LEFT JOIN #o_not_excluded_desc b ON a.concept_id = b.concept_id
LEFT JOIN #o_not_excl_non_std d ON a.concept_id = d.concept_id
LEFT JOIN @vocabulary_database_schema.concept c1 ON a.concept_id = c1.concept_id
LEFT JOIN @vocabulary_database_schema.concept c2 ON b.concept_id = c2.concept_id
LEFT JOIN @vocabulary_database_schema.concept c3 ON b.ancestor_concept_id = c3.concept_id
LEFT JOIN @vocabulary_database_schema.concept c4 ON d.available_standard_concept_id = c4.concept_id;



DROP TABLE IF EXISTS #o_concepts_excluded;
SELECT a.concept_id original_concept_id,
	a.invalid_reason,
	c1.concept_name original_concept_name,
	b.ancestor_concept_id,
	c3.concept_name ancestor_concept_name,
	cast(NULL AS INT) mapped_concept_id,
	cast(NULL AS VARCHAR(255)) mapped_concept_name,
	b.concept_id subsumed_concept_id,
	c2.concept_name subsumed_concept_name
INTO #o_concepts_excluded
FROM #o_excluded a
LEFT JOIN #o_excluded_desc b ON a.concept_id = b.concept_id
LEFT JOIN @vocabulary_database_schema.concept c1 ON a.concept_id = c1.concept_id
LEFT JOIN @vocabulary_database_schema.concept c2 ON b.concept_id = c2.concept_id
LEFT JOIN @vocabulary_database_schema.concept c3 ON b.ancestor_concept_id = c3.concept_id;


SELECT concept_id,
  concept_name,
  invalid_reason,
  excluded,
  removed
INTO #concepts_optimized
FROM (
	SELECT original_concept_id concept_id,
		original_concept_name concept_name,
		invalid_reason,
		cast(0 AS INT) excluded,
		cast(0 AS INT) removed
	FROM #o_concepts_included
	WHERE subsumed_concept_id = original_concept_id or
	subsumed_concept_id IS NULL
	UNION
	SELECT original_concept_id concept_id,
		original_concept_name concept_name,
		invalid_reason,
		cast(1 AS INT) excluded,
		cast(0 AS INT) removed
	FROM #o_concepts_excluded
	WHERE subsumed_concept_id = original_concept_id or
	subsumed_concept_id IS NULL
	) opt;
	
	

DROP TABLE IF EXISTS #concepts_removed;
SELECT concept_id,
  concept_name,
  invalid_reason,
  excluded,
  removed
INTO #concepts_removed
FROM (
	SELECT DISTINCT original_concept_id concept_id,
		original_concept_name concept_name,
		invalid_reason,
		cast(0 AS INT) excluded,
		cast(1 AS INT) removed
	FROM #o_concepts_included
	WHERE (subsumed_concept_id != original_concept_id and
	subsumed_concept_id IS NOT NULL)
	UNION
	SELECT DISTINCT original_concept_id concept_id,
		original_concept_name concept_name,
		invalid_reason,
		cast(1 AS INT) excluded,
		cast(1 AS INT) removed
	FROM #o_concepts_excluded
	WHERE (subsumed_concept_id != original_concept_id and
	subsumed_concept_id IS NOT NULL)
	) rmv;


SELECT *
INTO #optimized_set
FROM (
	SELECT concept_id,
		concept_name,
		invalid_reason,
		excluded,
		removed
	FROM #concepts_optimized
	UNION
	SELECT concept_id,
		concept_name,
		invalid_reason,
		excluded,
		removed
	FROM #concepts_removed
	) f
WHERE concept_id IS NOT NULL;


DROP TABLE IF EXISTS #o_not_excluded;
DROP TABLE IF EXISTS #o_not_excluded_desc;
DROP TABLE IF EXISTS #o_not_excl_non_std;
DROP TABLE IF EXISTS #o_excluded;
DROP TABLE IF EXISTS #o_excluded_desc;
DROP TABLE IF EXISTS #o_concepts_included;
DROP TABLE IF EXISTS #o_concepts_excluded;