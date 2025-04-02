{DEFAULT @migration = migration}
{DEFAULT @cohort_relationships = cohort_relationships}
{DEFAULT @cohort_count = cohort_count}
{DEFAULT @cohort = cohort}
{DEFAULT @temporal_covariate_ref = temporal_covariate_ref}
{DEFAULT @temporal_analysis_ref = temporal_analysis_ref}
{DEFAULT @temporal_covariate_value = temporal_covariate_value}
{DEFAULT @temporal_time_ref = temporal_time_ref}
{DEFAULT @table_prefix = ''}
{DEFAULT @analysis_id = 173}

-- Migrate data to FeatureExtraction output
INSERT INTO @database_schema.@table_prefix@temporal_covariate_ref
SELECT DISTINCT
       CAST ( CONCAT(CAST(cr.cohort_id AS varchar), '@analysis_id') AS bigint)  AS covariate_id,
       CONCAT('cohort:', c.cohort_name) AS covariate_name,
       @analysis_id as analysis_id,
       0 as concept_id
FROM @database_schema.@table_prefix@cohort_relationships cr
INNER JOIN @database_schema.@table_prefix@cohort c ON cr.cohort_id = c.cohort_id
;

INSERT INTO @database_schema.@table_prefix@temporal_analysis_ref
    (analysis_id, analysis_name, domain_id, is_binary)
SELECT
    DISTINCT
    @analysis_id as analysis_id,
    'CohortTemporal' as analysis_name,
    'cohort' as domain_id,
    'Y' as is_binary
FROM @database_schema.@table_prefix@cohort_relationships
-- only do this if cohort_relationships contains data
WHERE (SELECT COUNT(*) FROM @database_schema.@table_prefix@cohort_relationships) > 0;


INSERT INTO @database_schema.@table_prefix@temporal_covariate_value
    (DATABASE_ID, COVARIATE_ID, COHORT_ID,  SUM_VALUE,  MEAN,  SD, TIME_ID)
SELECT
    DISTINCT
    cr.database_id,
    CAST (CONCAT(CAST(cr.comparator_cohort_id AS varchar), '@analysis_id') AS bigint) AS covariate_id,
    cr.cohort_id,
    cr.subjects as sum_value, -- total in both cohorts
    CAST(cr.subjects as FLOAT) / CAST(cc.cohort_subjects as FLOAT) as mean,  -- fraction that overlap, can be used in characterization view
    sqrt(cr.subjects * (CAST(cr.subjects as FLOAT) / CAST(cc.cohort_subjects as FLOAT)) * (1 - (CAST(cr.subjects as FLOAT) / CAST(cc.cohort_subjects as FLOAT)))) as sd,
    ttr.time_id
FROM @database_schema.@table_prefix@cohort_relationships cr
INNER JOIN @database_schema.@table_prefix@cohort_count cc on cr.cohort_id = cc.cohort_id
INNER JOIN @database_schema.@table_prefix@temporal_time_ref ttr ON ttr.start_day = cr.start_day and ttr.end_day = cr.end_day
;

-- Remove old table
--DROP TABLE IF EXISTS @database_schema.@table_prefix@cohort_relationships;