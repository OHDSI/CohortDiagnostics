-- Migration: add cohort_start_year to index_event_breakdown
--
-- Issue #1197 (time series of index event breakdown).
-- The index event breakdown is now stratified by the calendar year of
-- cohort_start_date so that users can spot temporal shifts in index event
-- codes (for example a deprecated HCPCS code causing a drop in incidence).
--
-- cohort_start_year is part of the primary key: the breakdown SQL groups by
-- (concept, year), so a single concept legitimately produces one row per year
-- for the same cohort/database/domain combination. Adding the column without
-- extending the key means those rows collide on the existing constraint and
-- results upload fails with "duplicate key value violates unique constraint".
--
-- A primary key cannot be altered in place on all supported platforms, so the
-- table is rebuilt (rename, create with the new key, copy, drop) in the same
-- way as inst/sql/sqlite/migrations/Migration_2. Rows written by earlier
-- versions are preserved and backfilled with -1, meaning "year not recorded".

{DEFAULT @migration = migration}
{DEFAULT @index_event_breakdown = index_event_breakdown}
{DEFAULT @table_prefix = ''}

-- Rename the existing table so its rows can be copied across
ALTER TABLE @database_schema.@table_prefix@index_event_breakdown RENAME TO @table_prefix@index_event_breakdown_old;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @database_schema.@table_prefix@index_event_breakdown (
			concept_id BIGINT NOT NULL,
			concept_count BIGINT NOT NULL,
			subject_count BIGINT NOT NULL,
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			domain_field VARCHAR NOT NULL,
			domain_table VARCHAR NOT NULL,
			cohort_start_year INT NOT NULL,
			PRIMARY KEY(concept_id, cohort_id, database_id, domain_field, domain_table, cohort_start_year)
);

-- Copy any existing rows, tagging them as having no recorded year
INSERT INTO @database_schema.@table_prefix@index_event_breakdown
            (concept_id, concept_count, subject_count, cohort_id, database_id, domain_field, domain_table, cohort_start_year)
  SELECT concept_id, concept_count, subject_count, cohort_id, database_id, domain_field, domain_table, -1
  FROM @database_schema.@table_prefix@index_event_breakdown_old;

DROP TABLE @database_schema.@table_prefix@index_event_breakdown_old;
