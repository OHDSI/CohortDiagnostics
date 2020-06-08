SELECT cohort_definition_id AS cohort_id,
    COUNT(*) AS cohort_entries,
    COUNT(DISTINCT subject_id) AS cohort_subjects
FROM @cohort_database_schema.@cohort_table 
{@cohort_ids != ''} ? {WHERE cohort_definition_id IN (@cohort_ids)}
GROUP BY cohort_definition_id;
