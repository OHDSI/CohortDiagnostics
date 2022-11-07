# CohortDiagnostics Database Migrations

This directory contains migrations that are intended to be used to update schemas created with cohort diagnostics 
schemas.
It is assumed that the starting schema is version 3.0.x - all changes to the ddl referenced in the 
`CreateResultsDataModel.sql` script should be completed in with a database migration in the migrations folder.
The objective is to preserve data integrity whilst also allowing changes to data models.

For this reason migrations should only take place after a full backup of relevant databases/schemas.

All new tables should still be listed in the file `inst/settings/resultsDataModelSpecification.csv` - this should always
be an up-to-date reference for the data model.

## Creating a new migration File
Creating a new migration requires creating a file in the `sql/sql_server/Migration_<i>-*.sql` directory, where i is an intger 
- the pattern is strictly the regular expression `(Migration_[0-9]+)-(.+).sql`.

For example the file names will work:
```
Migration_2-MyMigration.sql

Migration_2-v3.2whaterver.sql

Migration_4-TEST.sql

Migration_4-2018922-vAAAA.sql
```

However, the following will not work:
```
MyMigration.sql # Does not include Migration_1 

Migration_2v3.2whaterver.sql # missing -

-TEST_Migration_1.sql # Wrong order

Migraton_4-a.sql # Migration spelt wrong
```


A new migration should, therefore always increment the number as order of execution is, naturally, essential.
When creating new migrations it is up to the developer  (and package maintainers) to increment this number responsibly.
The name should also be descriptive, as well as adding helpful comments for the migration at the top of the file.

## Setting parameters
Any table names or other variables should be set using the SqlRender parameter `{DEFAULT @table_name = @table_name}`.
The script will have the variables: 
```
@results_schema
@table_prefix
```

The use of `@table_prefix` when referencing tables is crucial.

## Adding script to list of migrations

The file must be stored in the migrations table. For example, at the bottom of the script the line is added:
```
INSERT INTO @results_schema.@table_prexif@migration (migration_completed, migration_order)
    VALUES ('Migration_1-v3_1_0_time_id.sql', 1);
```
Your unit tests will fail if the value in the `migration_file` field does not match your exact file.

## Version number updates
The version number of the DDL is tracked in file `inst/sql/sql_server/migrations/UpdateVersionNumber.sql`.
This is updated in the package maintenance script or manually when the package is released.
This is only intended to allow developers to debug the code.
This script is only executed after all migrations complete.

## Migrations for different platforms
It is likely that your data model change will not be supported by the `SqlRender` translation functionality.
For example, sqlite does not support renaming columns.
Instead, you must 
[recreate the entire table and copy the data](https://www.techonthenet.com/sqlite/tables/alter_table.php.).
See the file `inst/sql/sqlite/migrations/Migration_2-v3_1_0_ir_person_years.sql` for a working example.