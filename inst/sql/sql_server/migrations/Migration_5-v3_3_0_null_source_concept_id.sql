-- Set cohort name to larger field value
{DEFAULT @migration = migration}
{DEFAULT @included_source_concept = included_source_concept}
{DEFAULT @table_prefix = ''}

ALTER TABLE @database_schema.@table_prefix@included_source_concept ALTER COLUMN source_concept_id BIGINT NULL;
