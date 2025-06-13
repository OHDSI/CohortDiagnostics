{DEFAULT @time_bin_size = 7}
{DEFAULT @result_table = #time_dynamic_network}
{DEFAULT @co_occurrence_result_table = #co_occurrence_network}

-- Extract relevant records and assign to time bins
DROP TABLE IF EXISTS #time_bin_data;
CREATE TABLE #time_bin_data (
    cohort_definition_id BIGINT,
    person_id BIGINT,
    event_type VARCHAR(50),
    concept_id BIGINT,
    time_bin BIGINT
);

INSERT INTO #time_bin_data
SELECT
    c.cohort_definition_id,
    p.person_id,
    'procedure' AS event_type,
    procedure_concept_id AS concept_id,
    CEIL(DATEDIFF(DAY, c.cohort_start_date, procedure_date) / @time_bin_size) AS time_bin
FROM @cdm_database_schema.procedure_occurrence p
INNER JOIN @cohort_database_schema.@cohort_table_name c ON p.person_id = c.subject_id
WHERE c.cohort_definition_id IN (@cohort_ids)

UNION ALL

-- TODO: measurement ranges could be very useful here
SELECT
    c.cohort_definition_id,
    m.person_id,
    'measurement' AS event_type,
    measurement_concept_id AS concept_id,
    CEIL(DATEDIFF(DAY, c.cohort_start_date, measurement_date) / @time_bin_size) AS time_bin
FROM @cdm_database_schema.measurement m
INNER JOIN @cohort_database_schema.@cohort_table_name c ON m.person_id = c.subject_id
WHERE c.cohort_definition_id IN (@cohort_ids)

UNION ALL

SELECT
    c.cohort_definition_id,
    co.person_id,
    'condition' AS event_type,
    condition_concept_id AS concept_id,
    CEIL(DATEDIFF(DAY, c.cohort_start_date, condition_start_date) / @time_bin_size) AS time_bin
FROM @cdm_database_schema.condition_occurrence co
INNER JOIN @cohort_database_schema.@cohort_table_name c ON co.person_id = c.subject_id
WHERE c.cohort_definition_id IN (@cohort_ids)

UNION ALL

SELECT
    c.cohort_definition_id,
    de.person_id,
    'drug' AS event_type,
    drug_concept_id AS concept_id,
    CEIL(DATEDIFF(DAY, c.cohort_start_date, drug_era_start_date) / @time_bin_size) AS time_bin
FROM @cdm_database_schema.drug_era de
INNER JOIN @cohort_database_schema.@cohort_table_name c ON de.person_id = c.subject_id
WHERE c.cohort_definition_id IN (@cohort_ids)


UNION ALL

SELECT
    c.cohort_definition_id,
    vo.person_id,
    'visit context' AS event_type,
    visit_concept_id AS concept_id,
    CEIL(DATEDIFF(DAY, c.cohort_start_date, visit_start_date) / @time_bin_size) AS time_bin
FROM @cdm_database_schema.visit_occurrence vo
INNER JOIN @cohort_database_schema.@cohort_table_name c ON vo.person_id = c.subject_id
WHERE c.cohort_definition_id IN (@cohort_ids);


-- Co-ccurence network construction
-- Capture events that occur within the same time windows

DROP TABLE IF EXISTS #node_co_occurrences;
CREATE TABLE #node_co_occurrences (
    cohort_definition_id BIGINT,
    person_id BIGINT,
    time_bin BIGINT,
    concept_id_1 BIGINT,
    concept_id_2 BIGINT,
    cooccurrence_count BIGINT
);

INSERT INTO #node_co_occurrences
SELECT
    t1.cohort_definition_id,
    t1.person_id,
    t1.time_bin,
    t1.concept_id AS concept_id_1,
    t2.concept_id AS concept_id_2,
    COUNT(*) AS cooccurrence_count  -- Number of times a concept is shared by individuals within the same time bin
FROM time_bin_data t1
JOIN time_bin_data t2 ON t1.person_id = t2.person_id
    AND t1.time_bin = t2.time_bin
    AND t1.concept_id < t2.concept_id -- Prevent self-loop or duplicate edges
    AND t1.cohort_definition_id = t2.cohort_definition_id
GROUP BY t1.time_bin, t1.concept_id, t2.concept_id, t1.cohort_definition_id
;

DROP TABLE IF EXISTS @co_occurrence_result_table;
CREATE TABLE @co_occurrence_result_table (
    cohort_definition_id BIGINT,
    time_bin BIGINT,
    concept_id_1 BIGINT,
    concept_id_2 BIGINT,
    total_cooccurrence_count BIGINT
);

INSERT INTO @co_occurrence_result_table
SELECT
    cohort_definition_id,
    time_bin,
    concept_id_1,
    concept_id_2,
    SUM(cooccurrence_count) AS total_cooccurrence_count
FROM #node_co_occurrences
GROUP BY time_bin, concept_id_1, concept_id_2, cohort_definition_id
ORDER BY time_bin, total_cooccurrence_count DESC;


-- TIME DYNAMIC NETWORK CONSTRUCTION - capture states within time bins and store transitions between states
-- Create time-sequential edges between time bins for individuals, not to be exported

DROP TABLE IF EXISTS #time_edges;
CREATE TABLE #time_edges (
    cohort_definition_id BIGINT,
    person_id BIGINT,
    time_bin_1 BIGINT,
    concept_id_1 BIGINT,
    time_bin_2 BIGINT,
    concept_id_2 BIGINT
);


INSERT INTO #time_edges
SELECT
    t1.cohort_definition_id,
    t1.person_id,
    t1.time_bin AS time_bin_1,
    t1.concept_id AS concept_id_1,
    t2.time_bin AS time_bin_2,
    t2.concept_id AS concept_id_2
FROM time_bin_data t1
JOIN time_bin_data t2
    ON t1.person_id = t2.person_id  -- Same individual
    AND t2.time_bin = t1.time_bin + 1 -- Events in consecutive time bins
    AND t1.cohort_definition_id = t2.cohort_definition_id
;

DROP TABLE IF EXISTS @result_table;
CREATE TABLE @result_table (
    bin_size INT,
    cohort_definition_id BIGINT,
    time_bin_1 BIGINT,
    concept_id_1 BIGINT,
    time_bin_2 BIGINT,
    concept_id_2 BIGINT,
    edge_weight BIGINT
);

INSERT INTO @result_table
SELECT
    @time_bin_size as bin_size,
    cohort_definition_id,
    time_bin_1,
    concept_id_1,
    time_bin_2,
    concept_id_2,
    COUNT(*) AS edge_weight -- Number of transitions across different time points
FROM #time_edges
GROUP BY time_bin_1, concept_id_1, time_bin_2, concept_id_2, cohort_definition_id
ORDER BY time_bin_1, edge_weight;