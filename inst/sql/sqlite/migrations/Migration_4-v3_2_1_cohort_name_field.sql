-- changes cohort name to be a varchar max due to subset cohorts
{DEFAULT @migration = migration}
{DEFAULT @cohort = cohort}
{DEFAULT @table_prefix = ''}

ALTER TABLE @database_schema.@table_prefix@cohort RENAME TO _cohort_old;

CREATE TABLE @database_schema.@table_prefix@cohort (
    cohort_id BIGINT NOT NULL,
    cohort_name VARCHAR NOT NULL,
    metadata VARCHAR,
    sql VARCHAR NOT NULL,
    json VARCHAR NOT NULL,
    PRIMARY KEY(cohort_id)
);


INSERT INTO @database_schema.@table_prefix@cohort
            (cohort_id, cohort_name, metadata, sql, json)
SELECT cohort_id, cohort_name, metadata, sql, json
FROM _cohort_old;

DROP TABLE _cohort_old;
