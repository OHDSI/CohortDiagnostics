-- changes incidence_rate.person_years from bigint to float
{DEFAULT @migration = migration}
{DEFAULT @cohort = cohort}
{DEFAULT @subset_definition = subset_definition}
{DEFAULT @table_prefix = ''}

CREATE TABLE @database_schema.@table_prefix@subset_definition (
    subset_definition_id BIGINT,
    json varchar,
    PRIMARY KEY(subset_definition_id)
);


ALTER TABLE @database_schema.@table_prefix@cohort ADD COLUMN subset_definition_id BIGINT;
ALTER TABLE @database_schema.@table_prefix@cohort ADD COLUMN subset_parent BIGINT;
ALTER TABLE @database_schema.@table_prefix@cohort ADD COLUMN is_subset INT;
