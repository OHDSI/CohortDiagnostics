{DEFAULT @cd_version = cd_version}
{DEFAULT @cd_migrations = cd_migrations}
{DEFAULT @version_number = '3.1.0'}

-- Create table indicating version number of ddl
DROP TABLE IF EXISTS @results_schema.@cd_version;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @results_schema.@cd_version (
    version_number VARCHAR PRIMARY KEY
);

INSERT INTO @results_schema.@cd_version (version_number) VALUES ('@version_number');

-- Update time_ids in relevant tables
UPDATE TABLE @results_schema.@temporal_covariate_value SET time_id = 0 WHERE time_id IS NULL;
UPDATE TABLE @results_schema.@temporal_covariate_value_dist SET time_id = 0 WHERE time_id IS NULL;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @results_schema. (
    cd_migration_completed VARCHAR PRIMARY KEY --string value represents file name
);

-- If other statements fail, this won't update
INSERT INTO @results_schema.@cd_version (version_number) VALUES ('Migration_3_1_0_time_id.sql');