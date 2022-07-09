-- Database migrations for version 3.1.0
-- This migration updates the schema:
 -- 1. to store the cohort diagnostics version
 -- 2. converts NULL time_ids in temporal_covariate to 0
 -- 3. Add a migrations table for supporting database migrations

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @temporal_covariate_value = temporal_covariate_value}
{DEFAULT @temporal_covariate_value_dist = temporal_covariate_value_dist}
{DEFAULT @temporal_covariate_value_dist = temporal_covariate_value_dist}
{DEFAULT @incidence_rate = incidence_rate}
{DEFAULT @table_prefix = ''}

-- Create table indicating version number of ddl
DROP TABLE IF EXISTS @results_schema.@package_version;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @results_schema. @table_prefix@package_version (
    version_number VARCHAR PRIMARY KEY
);

-- Update time_ids in relevant tables
UPDATE @results_schema.@table_prefix@temporal_covariate_value SET time_id = 0 WHERE time_id IS NULL;
UPDATE @results_schema.@table_prefix@temporal_covariate_value_dist SET time_id = 0 WHERE time_id IS NULL;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @results_schema.@table_prefix@migration (
    migration_file VARCHAR PRIMARY KEY, --string value represents file name
    migration_order INT NOT NULL unique
);

-- If other statements fail, this won't update
INSERT INTO @results_schema.@table_prefix@migration (migration_file, migration_order)
    VALUES ('Migration_1-v3_1_0_time_id.sql', 1);