{DEFAULT @migration = migration}
{DEFAULT @cohort_overlap = cohort_overlap}
{DEFAULT @cohort_relationships = cohort_relationships}
{DEFAULT @table_prefix = ''}

DROP TABLE IF EXISTS @database_schema.@table_prefix@cohort_overlap;

ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_before_ts;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_on_ts;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_after_ts;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_before_te;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_on_te;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_after_te;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_window_t;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_ce_window_t;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_window_ts;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_cs_window_te;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_ce_window_ts;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_ce_window_te;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships DROP COLUMN rec_c_within_t;
