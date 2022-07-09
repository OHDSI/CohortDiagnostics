-- changes incidence_rate.person_years from bigint to float
{DEFAULT @migration = migration}
{DEFAULT @incidence_rate = incidence_rate}
{DEFAULT @table_prefix = ''}

-- ALTER TABLE @results_schema.@table_prefix@incidence_rate ALTER COLUMN person_years FLOAT;

ALTER TABLE @results_schema.@table_prefix@incidence_rate RENAME TO _incidence_rate_old;

CREATE TABLE @results_schema.@table_prefix@incidence_rate (
    cohort_count BIGINT NOT NULL,
    person_years NUMERIC,
    gender VARCHAR,
    age_group VARCHAR,
    calendar_year VARCHAR(4),
    incidence_rate NUMERIC NOT NULL,
    cohort_id BIGINT NOT NULL,
    database_id VARCHAR NOT NULL
);

INSERT INTO @results_schema.@table_prefix@incidence_rate
            (cohort_count, person_years, gender, age_group, calendar_year, incidence_rate, cohort_id, database_id)
  SELECT cohort_count, person_years, gender, age_group, calendar_year, incidence_rate, cohort_id, database_id
  FROM _incidence_rate_old;

DROP TABLE _incidence_rate_old;

-- If other statements fail, this won't update
INSERT INTO @results_schema.@table_prefix@migration (migration_file, migration_order)
    VALUES ('Migration_2-v3_1_0_ir_person_years.sql', 2);