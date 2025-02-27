{DEFAULT @migration = migration}
{DEFAULT @cohort_relationships = cohort_relationships}
{DEFAULT @table_prefix = ''}

-- TODO: Migration to FeatureExtraction output

-- Remove old table
DROP TABLE IF EXISTS @database_schema.@table_prefix@cohort_relationships;