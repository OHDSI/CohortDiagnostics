IF OBJECT_ID('tempdb..#inc_num') IS NOT NULL
  DROP TABLE #inc_num;

CREATE TABLE #inc_num with (distribution = replicate) AS
  SELECT
    YEAR(c.cohort_start_date) AS index_year,
    FLOOR((year(c.cohort_start_date) - p.year_of_birth) / 10) AS age_group_10y,
    p.gender_concept_id,
    {@first_occurrence_only} ? {
    COUNT(DISTINCT c.subject_id) AS person_count
    } : {
    COUNT(c.subject_id) AS person_count
    }
  FROM (
  {@first_occurrence_only} ? {
    SELECT
      cohort_definition_id,
      subject_id,
      MIN(cohort_start_date) cohort_start_date,
      MIN(cohort_end_date) cohort_end_date
    FROM @cohort_database_schema.@cohort_table
    WHERE cohort_definition_id = @cohort_definition_id
    GROUP BY
      cohort_definition_id,
      subject_id
  } : {
    SELECT
      *
    FROM @cohort_database_schema.@cohort_table
    WHERE cohort_definition_id = @cohort_definition_id
  }
  ) c
  INNER JOIN @cdm_database_schema.person p
    ON c.subject_id = p.person_id
  WHERE c.cohort_definition_id = @cohort_definition_id
  GROUP BY
    YEAR(c.cohort_start_date),
    FLOOR((year(c.cohort_start_date) - p.year_of_birth) / 10),
    p.gender_concept_id
;

IF OBJECT_ID('tempdb..#inc_denom') IS NOT NULL
  DROP TABLE #inc_denom;

CREATE TABLE #inc_denom with (distribution = replicate) AS
  SELECT
    iy.index_year,
    floor((iy.index_year - p.year_of_birth) / 10) AS age_group_10y,
    p.gender_concept_id,
    count(distinct p.person_id) AS person_count
  FROM @cdm_database_schema.observation_period op
  INNER JOIN (
    SELECT DISTINCT
      index_year
    FROM #inc_num
   ) iy
    ON year(op.observation_period_start_date) <= iy.index_year
    AND year(op.observation_period_end_date) >= iy.index_year
  INNER JOIN @cdm_database_schema.person p
    ON op.person_id = p.person_id
    AND datediff(d, op.observation_period_start_date, op.observation_period_end_date) > @min_observation_time
  GROUP BY iy.index_year,
    floor((iy.index_year - p.year_of_birth) / 10),
    p.gender_concept_id
;

IF OBJECT_ID('tempdb..#inc_denom_exclude') IS NOT NULL
  DROP TABLE #inc_denom_exclude;

CREATE TABLE #inc_denom_exclude with (distribution = replicate) AS
  SELECT
    iy.index_year,
    FLOOR((iy.index_year - p.year_of_birth) / 10) AS age_group_10y,
    p.gender_concept_id,
    COUNT(DISTINCT p.person_id) AS person_count
  FROM @cdm_database_schema.observation_period op
  INNER JOIN (
    SELECT
      subject_id,
      MIN(cohort_start_date) AS cohort_start_date
    FROM @cohort_database_schema.@cohort_table
    WHERE cohort_definition_id = @cohort_definition_id
    GROUP BY subject_id
  ) c
    ON op.person_id = c.subject_id
  INNER JOIN (
    SELECT DISTINCT
      index_year
    FROM #inc_num
  ) iy
    ON year(op.observation_period_start_date) <= iy.index_year
    AND year(op.observation_period_end_date) >= iy.index_year
    AND year(c.cohort_start_date) < iy.index_year
  INNER JOIN @cdm_database_schema.person p
    ON op.person_id = p.person_id
    AND datediff(d, op.observation_period_start_date, op.observation_period_end_date) > @min_observation_time
  GROUP BY iy.index_year,
    floor((iy.index_year - p.year_of_birth) / 10),
    p.gender_concept_id
;

IF OBJECT_ID('tempdb..#inc_summary') IS NOT NULL
  DROP TABLE #inc_summary;

CREATE TABLE #inc_summary with (distribution = replicate) AS
  SELECT in1.index_year,
    in1.age_group_10y,
    c1.concept_name AS gender,
    in1.person_count AS num_count,
    {@first_occurrence_only} ? {
    id1.person_count - CASE WHEN ide1.person_count IS NULL then 0 ELSE ide1.person_count END AS denom_count,
    1000.0 * in1.person_count / (id1.person_count - CASE WHEN ide1.person_count is NULL THEN 0 ELSE ide1.person_count END) AS ip_1000p
    } : {
    id1.person_count AS denom_count,
    1000.0 * in1.person_count / id1.person_count AS ip_1000p
    }
  FROM #inc_num in1
  INNER JOIN #inc_denom id1
    ON in1.index_year = id1.index_year
    AND in1.age_group_10y = id1.age_group_10y
    AND in1.gender_concept_id = id1.gender_concept_id
  {@first_occurrence_only} ? {
  LEFT JOIN #inc_denom_exclude ide1
    ON in1.index_year = ide1.index_year
    AND in1.age_group_10y = ide1.age_group_10y
    AND in1.gender_concept_id = ide1.gender_concept_id
  } 
  INNER JOIN @cdm_database_schema.concept c1
    ON in1.gender_concept_id = c1.concept_id
;
