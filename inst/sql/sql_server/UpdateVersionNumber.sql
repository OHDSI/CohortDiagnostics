{DEFAULT @package_version = package_version}
{DEFAULT @version_number = '3.1.0'}

DELETE FROM @results_schema.@table_prefix@package_version;
INSERT INTO @results_schema.@table_prefix@package_version (version_number) VALUES ('@version_number');