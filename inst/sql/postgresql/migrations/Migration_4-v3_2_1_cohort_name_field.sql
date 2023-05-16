-- Set cohort name to larger field value
{DEFAULT @migration = migration}
{DEFAULT @cohort = cohort}
{DEFAULT @table_prefix = ''}

ALTER TABLE @database_schema.@table_prefix@cohort ALTER COLUMN cohort_name TYPE VARCHAR;
