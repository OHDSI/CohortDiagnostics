DROP TABLE IF EXISTS #ts_cohort;
DROP TABLE IF EXISTS #ts_cohort_first;
DROP TABLE IF EXISTS #ts_output;

--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT *
INTO #ts_cohort
FROM @cohort_database_schema.@cohort_table {@cohort_ids != '' } ? {
WHERE cohort_definition_id IN (@cohort_ids) };

--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT cohort_definition_id,
	subject_id,
	min(cohort_start_date) cohort_start_date,
	min(cohort_end_date) cohort_end_date
INTO #ts_cohort_first
FROM #ts_cohort
GROUP BY cohort_definition_id,
	subject_id;


SELECT c.*,
	CASE
		WHEN c.cohort_start_date = cf.cohort_start_date
			THEN 'Y'
		ELSE 'N'
		END first_occurrence {@stratify_by_gender} ? {,
	concept.concept_name gender} {@stratify_by_age_group} ? {,
	p.year_of_birth}
INTO #cohort_ts
FROM #ts_cohort c
INNER JOIN #ts_cohort_first cf
	ON c.cohort_definition_id = cf.cohort_definition_id
		AND c.subject_id = cf.subject_id {@stratify_by_gender | @stratify_by_age_group} ? {
INNER JOIN @cdm_database_schema.person p
	ON c.subject_id = p.person_id} {@stratify_by_gender} ? {
INNER JOIN @cdm_database_schema.concept
	ON p.gender_concept_id = concept.concept_id};

DROP TABLE IF EXISTS #ts_cohort;
DROP TABLE IF EXISTS #ts_cohort_first;