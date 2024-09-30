{DEFAULT @migration = migration}
{DEFAULT @cohort_overlap = cohort_overlap}
{DEFAULT @cohort_relationships = cohort_relationships}
{DEFAULT @table_prefix = ''}

DROP TABLE IF EXISTS @database_schema.@table_prefix@cohort_overlap;

CREATE TABLE @database_schema.@table_prefix@cohort_relationships_new (
    database_id VARCHAR NOT NULL,
    cohort_id BIGINT NOT NULL,
    comparator_cohort_id BIGINT NOT NULL,
    start_day BIGINT NOT NULL,
    end_day BIGINT NOT NULL,
    subjects BIGINT NOT NULL,
    sub_cs_before_ts BIGINT NOT NULL,
    sub_cs_on_ts BIGINT NOT NULL,
    sub_cs_after_ts BIGINT NOT NULL,
    sub_cs_before_te BIGINT NOT NULL,
    sub_cs_on_te BIGINT NOT NULL,
    sub_cs_after_te BIGINT NOT NULL,
    sub_cs_window_t BIGINT NOT NULL,
    sub_ce_window_t BIGINT NOT NULL,
    sub_cs_window_ts BIGINT NOT NULL,
    sub_cs_window_te BIGINT NOT NULL,
    sub_ce_window_ts BIGINT NOT NULL,
    sub_ce_window_te BIGINT NOT NULL,
    sub_c_within_t BIGINT NOT NULL,
    c_days_before_ts BIGINT NOT NULL,
    c_days_before_te BIGINT NOT NULL,
    c_days_within_t_days BIGINT NOT NULL,
    c_days_after_ts BIGINT NOT NULL,
    c_days_after_te BIGINT NOT NULL,
    t_days BIGINT NOT NULL,
    c_days BIGINT NOT NULL,
    PRIMARY KEY(database_id, cohort_id, comparator_cohort_id, start_day, end_day)
);

INSERT INTO  @database_schema.@table_prefix@cohort_relationships_new
SELECT
    database_id ,
    cohort_id,
    comparator_cohort_id,
    start_day,
    end_day,
    subjects,
    sub_cs_before_ts,
    sub_cs_on_ts,
    sub_cs_after_ts,
    sub_cs_before_te,
    sub_cs_on_te,
    sub_cs_after_te,
    sub_cs_window_t,
    sub_ce_window_t,
    sub_cs_window_ts,
    sub_cs_window_te,
    sub_ce_window_ts,
    sub_ce_window_te,
    sub_c_within_t,
    c_days_before_ts,
    c_days_before_te,
    c_days_within_t_days,
    c_days_after_ts,
    c_days_after_te,
    t_days,
    c_days
    FROM @database_schema.@table_prefix@cohort_relationships;

DROP TABLE @database_schema.@table_prefix@cohort_relationships;
ALTER TABLE @database_schema.@table_prefix@cohort_relationships_new RENAME TO @table_prefix@cohort_relationships;
