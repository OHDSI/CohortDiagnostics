DROP TABLE IF EXISTS #target_cohort_table;
DROP TABLE IF EXISTS #cohort_rel_output;

-- target cohort: cohort relationship uses the first occurrence 
SELECT cohort_definition_id,
        subject_id,
        MIN(cohort_start_date) cohort_start_date,
        MIN(cohort_end_date) cohort_end_date
INTO #target_cohort_table
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id IN (@target_cohort_ids)
GROUP BY cohort_definition_id,
          subject_id;


-- target cohort: always one subject per cohort (first time)
SELECT t.cohort_definition_id cohort_id,
	c.cohort_definition_id comparator_cohort_id,
	CAST(@time_id AS INT) time_id,
	COUNT_BIG(DISTINCT c.subject_id) subjects,
	-- present in both target and comparator
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_before_ts,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_before_ts,
	-- comparator cohort start date before target start date (offset) [How many subjects in comparator cohort start prior to first target cohort start]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_on_ts,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_on_ts,
	-- comparator cohort start date on target start date (offset) [How many subjects in comparator cohort start with first target cohort start]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_after_ts,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_after_ts,
	-- comparator cohort start date after target start date (offset) [How many subjects in comparator cohort start after first target cohort start]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_before_te,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_before_te,
	-- comparator cohort start date before target end date (offset) [How many subjects in comparator cohort start after first target cohort end]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_on_te,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_on_te,
	-- comparator cohort start date on target end date (offset) [How many subjects in comparator cohort start on first target cohort end]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_after_te,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_after_te,
	-- comparator cohort start date after target end date (offset) [How many subjects in comparator cohort start after first target cohort end]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_window_t,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_window_t,
	-- comparator cohort subjects start within period (incidence) relative to target start date and end date
	-- [How many subjects in comparator cohort start within a window of days relative to first target start date and first target end date]
		COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_ce_window_t,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_ce_window_t,
	-- comparator cohort subjects start within period (incidence) relative to target start date and end date
	-- [How many subjects in comparator cohort start within a window of days relative to first target start date and first target end date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_window_ts,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_start_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_window_ts,
	-- comparator cohort subjects start within period (incidence) relative to target start date [How many subjects in comparator cohort start within a 
	-- window of days relative to first target start date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_end_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_window_te,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_end_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_cs_window_te,
	-- comparator cohort subjects start within period (incidence) relative to target end date [How many subjects in comparator cohort start within a 
	-- window of days relative to first target end date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_ce_window_ts,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_start_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_ce_window_ts,
	-- comparator cohort subjects end within period (incidence) relative to target start date [How many subjects in comparator cohort end within a 
	-- window of days relative to first target start date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_end_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_ce_window_te,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_end_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_ce_window_te,
	-- comparator cohort subjects end within period (incidence) relative to target end date [How many subjects in comparator cohort end within a 
	-- window of days relative to first target end date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				AND c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_c_within_t,
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				AND c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN CONCAT(cast(c.subject_id AS VARCHAR(30)), '_', cast(c.cohort_start_date AS VARCHAR(30)))
			ELSE NULL
			END) rec_c_within_t,
	-- comparator cohort days within target (offset) days [How many subjects in comparator cohort have their entire cohort period within first target cohort period]
	SUM((
			CASE -- comparator cohort start date before target start date (offset)
				WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
					THEN datediff(dd,
					              c.cohort_start_date, 
					              CASE --min of comparator end date/target start dates (offset)
          								WHEN c.cohort_end_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
          									THEN c.cohort_end_date
          								ELSE DATEADD(day, @start_day_offset, t.cohort_start_date)
								          END)
				ELSE 0
				END
			) + 1) c_days_before_ts,
	-- comparator cohort days before target start date (offset)
	SUM((
			CASE -- comparator cohort start date before target end date (offset)
				WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_end_date)
					THEN datediff(dd, 
					              c.cohort_start_date, 
					              CASE --min of comparator end date/target end dates (offset)
								          WHEN c.cohort_end_date < DATEADD(day, @start_day_offset, t.cohort_end_date)
									          THEN c.cohort_end_date
								          ELSE DATEADD(day, @start_day_offset, t.cohort_end_date)
								          END)
				ELSE 0
				END
			) + 1) c_days_before_te,
	-- comparator cohort days before target end date (offset)
	SUM((
			CASE -- comparator cohort days within target days (offset)
				WHEN  c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
					    AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
					THEN datediff(dd, 
					              CASE --min of comparator start date/target start dates (offset)
								            WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
									          THEN DATEADD(day, @start_day_offset, t.cohort_start_date)
								          ELSE c.cohort_start_date
								          END, 
								        CASE --min of comparator end date/target end dates (offset)
								            WHEN c.cohort_end_date > DATEADD(day, @end_day_offset, t.cohort_end_date)
									          THEN DATEADD(day, @end_day_offset, t.cohort_end_date)
								          ELSE c.cohort_end_date
								          END)
				ELSE 0
				END
			) + 1) c_days_within_t_days,
	-- comparator cohort days within target cohort days (offset)
		SUM((
			CASE -- comparator cohort end date after target start date (offset)
				WHEN c.cohort_end_date > DATEADD(day, @start_day_offset, t.cohort_start_date)
					THEN datediff(dd, 
					              CASE --max of comparator start date/target start dates (offset)
								            WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
									          THEN DATEADD(day, @start_day_offset, t.cohort_start_date)
								          ELSE c.cohort_start_date
								          END,
								          c.cohort_end_date)
				ELSE 0
				END
			) + 1) c_days_after_ts,
	-- comparator cohort days after target start date (offset)
		SUM((
			CASE -- comparator cohort end date after target end date (offset)
				WHEN c.cohort_end_date > DATEADD(day, @start_day_offset, t.cohort_end_date)
					THEN datediff(dd, 
					              CASE --max of comparator start date/target start dates (offset)
								            WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_end_date)
									          THEN DATEADD(day, @start_day_offset, t.cohort_end_date)
								          ELSE c.cohort_start_date
								          END,
								          c.cohort_end_date)
				ELSE 0
				END
			) + 1) c_days_after_te,
	-- comparator cohort days after target end date (offset)
	SUM(datediff(dd, DATEADD(day, @start_day_offset, t.cohort_start_date), DATEADD(day, @end_day_offset, t.cohort_end_date)) + 1) t_days,
	-- target cohort days (no offset)
	SUM(datediff(dd, c.cohort_start_date, c.cohort_end_date) + 1) c_days
-- comparator cohort days (offset)
INTO #cohort_rel_output
FROM #target_cohort_table t
INNER JOIN @cohort_database_schema.@cohort_table c ON c.subject_id = t.subject_id
	AND c.cohort_definition_id != t.cohort_definition_id
	-- comparator cohort overlaps with target cohort during the offset period
	AND c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
	AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
WHERE t.cohort_definition_id IN (@target_cohort_ids)
    AND c.cohort_definition_id IN (@comparator_cohort_ids)
GROUP BY t.cohort_definition_id,
	c.cohort_definition_id;

DROP TABLE IF EXISTS #target_cohort_table;
