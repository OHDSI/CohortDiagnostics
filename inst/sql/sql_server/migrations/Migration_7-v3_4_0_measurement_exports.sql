{DEFAULT @migration = migration}
{DEFAULT @temporal_covariate_ref = temporal_covariate_ref}
{DEFAULT @table_prefix = ''}

ALTER TABLE @database_schema.@table_prefix@temporal_covariate_ref ADD value_as_concept_id BIGINT;
