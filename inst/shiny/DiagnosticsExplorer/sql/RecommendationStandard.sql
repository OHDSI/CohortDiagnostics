WITH concept_set 
AS (
	@concept_set_query
	), 
list
AS (
	SELECT concept.concept_id,
		concept_name,
		domain_id,
		vocabulary_id,
		standard_concept
	FROM @vocabulary_database_schema.concept
	INNER JOIN concept_set
		ON concept_set.concept_id = concept.concept_id
	),
recommendations
AS (
	--list all included concepts
	SELECT i.concept_id,
		'Included' AS concept_in_set
	FROM list i
	
	UNION
	
	--find not included concepts found by orphan check via standards
	SELECT rc1.concept_id,
		'Not included - recommended via standard' AS concept_in_set
	FROM list i
	JOIN @target_database_schema.recommender_set rc1
		ON i.concept_id = rc1.source_id
	JOIN @vocabulary_database_schema.concept c1
		ON rc1.concept_id = c1.concept_id
			AND c1.standard_concept = 'S'
	
	UNION
	
	SELECT cr1.concept_id_2,
		'Not included - recommended via source' AS concept_in_set
	FROM list i
	JOIN @target_database_schema.recommender_set rc1
		ON i.concept_id = rc1.source_id
	JOIN @vocabulary_database_schema.concept c1
		ON rc1.concept_id = c1.concept_id
			AND c1.standard_concept IS NULL
	JOIN @vocabulary_database_schema.concept_relationship cr1
		ON c1.concept_id = cr1.concept_id_1
			AND cr1.relationship_id IN (
				'Maps to',
				'Maps to value'
				)
	-- excluding those sources that already have one standard counterpart in our input list
	LEFT JOIN (
		SELECT *
		FROM list l
		JOIN @vocabulary_database_schema.concept_relationship cr2
			ON l.concept_id = cr2.concept_id_2
				AND cr2.relationship_id = 'Maps to'
		) a
		ON a.concept_id_1 = cr1.concept_id_1
	WHERE a.concept_id_2 IS NULL
	
	UNION
	
	-- find all not included parents
	SELECT ca.ancestor_concept_id,
		'Not included - parent' AS concept_in_set
	FROM list i
	JOIN @vocabulary_database_schema.concept_ancestor ca
		ON i.concept_id = ca.descendant_concept_id
			AND ca.min_levels_of_separation = 1
	
	UNION
	
	-- find all not included children
	SELECT ca.descendant_concept_id,
		'Not included - descendant' AS concept_in_set
	FROM list i
	JOIN @vocabulary_database_schema.concept_ancestor ca
		ON i.concept_id = ca.ancestor_concept_id
	)
SELECT c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.domain_id,
	c.standard_concept,
	concept_in_set,
	COALESCE(cp.rc, 0) AS record_count,
	COALESCE(cp.dbc, 0) AS database_count,
	COALESCE(cp.drc, 0) AS descendant_record_count,
	COALESCE(cp.ddbc, 0) AS descendant_database_count
FROM recommendations r
JOIN @vocabulary_database_schema.concept c
	ON c.concept_id = r.concept_id
LEFT JOIN @target_database_schema.cp_master cp
	ON r.concept_id = cp.concept_id
LEFT JOIN @target_database_schema.recommended_blacklist rb
	ON r.concept_id = rb.concept_id
WHERE (
		rb.concept_id IS NULL
		AND NOT EXISTS (
			SELECT 1
			FROM list l
			JOIN @vocabulary_database_schema.concept_relationship cr1
				ON l.concept_id = cr1.concept_id_2
					AND cr1.relationship_id = 'Maps to'
			WHERE cr1.concept_id_1 = r.concept_id
			)
		OR concept_in_set = 'Included'
		);
