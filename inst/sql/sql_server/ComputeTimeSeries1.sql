-- cohort time series T1: subjects in the cohort who have atleast one cohort day in calendar period
--- (i.e. cohort start or cohort end is between (inclusive) calendar period, or 
--- (cohort start is on/before calendar start AND cohort end is on/after calendar end))

{DEFAULT @stratify_by_gender = FALSE}
{DEFAULT @stratify_by_age_group = FALSE}

SELECT cohort_definition_id cohort_id,
	time_id,
	{@stratify_by_gender} ? { CASE WHEN gender IS NULL THEN 'NULL' ELSE gender END} : {'NULL'} gender,
	{@stratify_by_age_group} ? {FLOOR((YEAR(period_begin) - year_of_birth) / 10) age_group,} : {CAST(NULL AS INT) age_group, }
	COUNT_BIG(DISTINCT CONCAT(cast(subject_id AS VARCHAR(30)), '_', cast(cohort_start_date AS VARCHAR(30)))) records,
	-- records in calendar period
	COUNT_BIG(DISTINCT subject_id) subjects,
	-- unique subjects in calendar period
	SUM(datediff(dd, CASE 
				WHEN cohort_start_date >= period_begin
					THEN cohort_start_date
				ELSE period_begin
				END, CASE 
				WHEN cohort_end_date >= period_end
					THEN period_end
				ELSE cohort_end_date
				END) + 1) person_days,
	-- person days within period
	SUM(CASE WHEN first_occurrence = 'Y' -- incident
	    THEN datediff(dd, CASE 
                				WHEN cohort_start_date >= period_begin
					                THEN cohort_start_date
                				ELSE period_begin
                				END, 
                				CASE 
                				WHEN cohort_end_date >= period_end
                					THEN period_end
                				ELSE cohort_end_date
                				END
                		) + 1
      ELSE 0 
      END) person_days_in,
	-- person days within period - incident
	COUNT_BIG(CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN CONCAT(cast(subject_id AS VARCHAR(30)), '_', cast(cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) records_start,
	-- records start within period
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_start,
	-- subjects start within period
	COUNT_BIG(DISTINCT CASE 
			WHEN first_occurrence = 'Y' -- incident
				AND cohort_start_date >= period_begin
				AND cohort_start_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_start_in,
	-- subjects start within period - incidence
	COUNT_BIG(CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN CONCAT(cast(subject_id AS VARCHAR(30)), '_', cast(cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) records_end,
	-- records end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_end, -- subjects end within period
	COUNT_BIG(DISTINCT CASE 
			WHEN first_occurrence = 'Y' -- incident
				AND cohort_end_date >= period_begin
				AND cohort_end_date <= period_end
				THEN subject_id
			ELSE NULL
			END) subjects_end_in -- subjects end within period
FROM #cohort_ts
INNER JOIN #calendar_periods cp ON cp.period_end >= cohort_start_date
	AND cp.period_begin <= cohort_end_date
{@stratify_by_age_group} ? {WHERE year_of_birth <= YEAR(period_begin)}
GROUP BY time_id,
	cohort_definition_id
	{@stratify_by_gender} ? {, gender}
	{@stratify_by_age_group} ? {, FLOOR((YEAR(period_begin) - year_of_birth) / 10)}
	;
