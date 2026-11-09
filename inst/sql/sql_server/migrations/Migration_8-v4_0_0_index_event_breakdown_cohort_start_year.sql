-- Migration: add cohort_start_year to index_event_breakdown
--
-- Issue #1197 (time series of index event breakdown).
-- The index event breakdown is now stratified by the calendar year of
-- cohort_start_date so that users can spot temporal shifts in index event
-- codes (for example a deprecated HCPCS code causing a drop in incidence).
--
-- Existing rows are backfilled with -1, which marks them as
-- "year not recorded" for results produced by earlier versions.

{DEFAULT @migration = migration}
{DEFAULT @index_event_breakdown = index_event_breakdown}
{DEFAULT @table_prefix = ''}

ALTER TABLE @database_schema.@table_prefix@index_event_breakdown ADD cohort_start_year INT DEFAULT -1;
