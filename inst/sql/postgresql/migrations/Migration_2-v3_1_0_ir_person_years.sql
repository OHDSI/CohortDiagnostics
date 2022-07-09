-- changes incidence_rate.person_years from bigint to float
{DEFAULT @migration = migration}
{DEFAULT @incidence_rate = incidence_rate}
{DEFAULT @table_prefix = ''}

ALTER TABLE @results_schema.@table_prefix@incidence_rate ALTER COLUMN person_years TYPE NUMERIC;

-- If other statements fail, this won't update
INSERT INTO @results_schema.@table_prefix@migration (migration_file, migration_order)
    VALUES ('Migration_2-v3_1_0_ir_person_years.sql', 2);