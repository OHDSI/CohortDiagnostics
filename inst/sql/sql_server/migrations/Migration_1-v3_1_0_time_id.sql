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
DROP TABLE IF EXISTS @database_schema.@table_prefix@package_version;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @database_schema.@table_prefix@package_version (
    version_number VARCHAR(50) PRIMARY KEY
);

-- Update time_ids in relevant tables
UPDATE @database_schema.@table_prefix@temporal_covariate_value SET time_id = 0 WHERE time_id IS NULL;
UPDATE @database_schema.@table_prefix@temporal_covariate_value_dist SET time_id = 0 WHERE time_id IS NULL;
