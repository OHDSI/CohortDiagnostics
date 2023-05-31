-- database time series T3: persons in the data source who have atleast one observation day in calendar period
--- (i.e. observation start or observation end is between (inclusive) calendar period, or 
--- (observation start is on/before calendar start AND observation end is on/after calendar end))

{DEFAULT @stratify_by_gender = FALSE}
{DEFAULT @stratify_by_age_group = FALSE}

SELECT -44819062 cohort_id,
	time_id,
	{@stratify_by_gender} ? { CASE WHEN p.gender IS NULL THEN 'NULL' ELSE p.gender END} : {'NULL'} gender,
	{@stratify_by_age_group} ? {FLOOR((YEAR(period_begin) - year_of_birth) / 10) AS age_group,} : {CAST(NULL AS INT) age_group, }
	COUNT_BIG(DISTINCT CONCAT(cast(o.person_id AS VARCHAR(30)), '_', cast(observation_period_start_date AS VARCHAR(30)))) records, -- records in calendar month
	COUNT_BIG(DISTINCT o.person_id) subjects, -- unique subjects
	SUM(datediff(dd, CASE 
				WHEN observation_period_start_date >= period_begin
					THEN observation_period_start_date
				ELSE period_begin
				END, CASE 
				WHEN observation_period_end_date >= period_end
					THEN period_end
				ELSE observation_period_end_date
				END) + 1) person_days,
	0 person_days_in,
	COUNT_BIG(CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN CONCAT(cast(o.person_id AS VARCHAR(30)), '_', cast(observation_period_start_date AS VARCHAR(30)))
			ELSE NULL
			END) records_start,
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_start_date >= period_begin
				AND observation_period_start_date <= period_end
				THEN o.person_id
			ELSE NULL
			END) subjects_start,
	0 subjects_start_in,
	COUNT_BIG(CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN CONCAT(cast(o.person_id AS VARCHAR(30)), '_', cast(observation_period_start_date AS VARCHAR(30)))
			ELSE NULL
			END) records_end, -- records end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN observation_period_end_date >= period_begin
				AND observation_period_end_date <= period_end
				THEN o.person_id
			ELSE NULL
			END) subjects_end, -- subjects end within period
	0 subjects_end_in
FROM @cdm_database_schema.observation_period o
INNER JOIN #calendar_periods cp ON cp.period_end >= observation_period_start_date
	AND cp.period_begin <= observation_period_end_date
	{@stratify_by_gender | @stratify_by_age_group} ? {
INNER JOIN
	(
		SELECT p1.person_id
      {@stratify_by_age_group} ? {, p1.year_of_birth}
		  {@stratify_by_gender} ? {, c.concept_name gender}
		FROM @cdm_database_schema.person p1
		  {@stratify_by_gender} ? {
		INNER JOIN @cdm_database_schema.concept c 
		ON p1.gender_concept_id = c.concept_id}) p
  ON o.person_id = p.person_id
}
{@stratify_by_age_group} ? {WHERE year_of_birth <= YEAR(period_begin)}
GROUP BY time_id
	{@stratify_by_gender} ? {, gender}
	{@stratify_by_age_group} ? {, FLOOR((YEAR(period_begin) - year_of_birth) / 10)};
