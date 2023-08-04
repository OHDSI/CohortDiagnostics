-- cohort time series T2: subjects in the cohort who have atleast one observation day in calendar period
--- (i.e. observation start or observation end is between (inclusive) calendar period, or 
--- (observation start is on/before calendar start AND observation end is on/after calendar end))
-- subjects in the cohort whose observation period overlaps calendar period

{DEFAULT @stratify_by_gender = FALSE}
{DEFAULT @stratify_by_age_group = FALSE}

SELECT cohort_definition_id cohort_id,
	time_id,
	{@stratify_by_gender} ? {CASE WHEN gender IS NULL THEN 'NULL' ELSE gender END} : {'NULL'} gender,
	{@stratify_by_age_group} ? {FLOOR((YEAR(period_begin) - year_of_birth) / 10) AS age_group,} : {CAST(NULL AS INT) age_group, }
	COUNT_BIG(DISTINCT CONCAT(cast(subject_id AS VARCHAR(30)), '_', cast(observation_period_start_date AS VARCHAR(30)))) records, -- records in calendar month
	COUNT_BIG(DISTINCT subject_id) subjects, -- unique subjects
	SUM(datediff(dd, CASE 
				WHEN observation_period_start_date >= period_begin
					THEN observation_period_start_date
				ELSE period_begin
				END, CASE 
				WHEN observation_period_end_date >= period_end
					THEN period_end
				ELSE observation_period_end_date
				END) + 1) person_days, -- person days within period
	0 person_days_in, -- person days within period - incident
	COUNT_BIG(CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN CONCAT(cast(subject_id AS VARCHAR(30)), '_', cast(observation_period_start_date AS VARCHAR(30)))
			ELSE NULL
			END) records_start,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_start,
	0 subjects_start_in,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN CONCAT(cast(subject_id AS VARCHAR(30)), '_', cast(observation_period_start_date AS VARCHAR(30)))
			ELSE NULL
			END) records_end, -- records end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_end, -- persons end within period
	0 subjects_end_in
FROM @cdm_database_schema.observation_period o
INNER JOIN (
	-- limiting to the cohort
	SELECT DISTINCT cohort_definition_id,
		subject_id
		{@stratify_by_gender} ? {, gender}
		{@stratify_by_age_group} ? {, year_of_birth}
	FROM #cohort_ts
	) c ON o.person_id = c.subject_id
INNER JOIN #calendar_periods cp ON cp.period_end >= observation_period_start_date
	AND cp.period_begin <= observation_period_end_date
WHERE cohort_definition_id >=0
  {@stratify_by_age_group} ? {AND year_of_birth <= YEAR(period_begin)}
GROUP BY time_id,
	cohort_definition_id
	{@stratify_by_gender} ? {, gender}
	{@stratify_by_age_group} ? {, FLOOR((YEAR(period_begin) - year_of_birth) / 10)};
