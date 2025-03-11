{DEFAULT @migration = migration}
{DEFAULT @cohort_overlap = cohort_overlap}
{DEFAULT @cohort_relationships = cohort_relationships}
{DEFAULT @table_prefix = ''}

ALTER TABLE @database_schema.@table_prefix@temporal_covariate_ref ADD value_as_concept_id BIGINT;