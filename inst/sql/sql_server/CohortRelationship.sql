SELECT t.cohort_definition_id cohort_id,
	c.cohort_definition_id comparator_cohort_id,
	@time_id time_id,
	COUNT_BIG(DISTINCT c.subject_id) subjects,
	-- present in both target and comparator
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_before_ts,
	-- comparator cohort start date before target start date (offset) [How many subjects in comparator cohort start prior to target cohort start]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_on_ts,
	-- comparator cohort start date on target start date (offset) [How many subjects in comparator cohort start with target cohort start]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, @start_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_after_ts,
	-- comparator cohort start date after target start date (offset) [How many subjects in comparator cohort start after target cohort start]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_before_te,
	-- comparator cohort start date before target end date (offset) [How many subjects in comparator cohort start after target cohort end]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date = DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_on_te,
	-- comparator cohort start date on target end date (offset) [How many subjects in comparator cohort start on target cohort end]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date > DATEADD(day, @start_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_after_te,
	-- comparator cohort start date after target end date (offset) [How many subjects in comparator cohort start after target cohort end]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_window_ts,
	-- comparator cohort subjects start within period (incidence) relative to target start date [How many subjects in comparator cohort start within a window of days relative to target start date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_end_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_cs_window_te,
	-- comparator cohort subjects start within period (incidence) relative to target end date [How many subjects in comparator cohort start within a window of days relative to target end date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_start_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_ce_window_ts,
	-- comparator cohort subjects end within period (incidence) relative to target start date [How many subjects in comparator cohort end within a window of days relative to target start date]
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_end_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_ce_window_te,
	-- comparator cohort subjects end within period (incidence) relative to target end date [How many subjects in comparator cohort end within a window of days relative to target end date]
	
	COUNT_BIG(DISTINCT CASE 
			WHEN c.cohort_start_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				AND c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
				AND c.cohort_end_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
				THEN c.subject_id
			ELSE NULL
			END) sub_c_within_t
-- comparator cohort days within target (offset) days [How many subjects in comparator cohort have their entire cohort period within target cohort period]
FROM #target_subset t
INNER JOIN #comparator_subset c ON c.subject_id = t.subject_id
	AND c.cohort_definition_id != t.cohort_definition_id
--	AND c.cohort_end_date >= t.cohort_start_date --DATEADD(day, @start_day_offset, t.cohort_start_date)
--	AND c.cohort_start_date <= t.cohort_end_date --DATEADD(day, @end_day_offset, t.cohort_end_date)
GROUP BY t.cohort_definition_id,
	c.cohort_definition_id;
	-- It is most probably not possible to compute person days for event cohorts with this SQL logic.
	-- commenting it out for now, might revisit in future
	--,COUNT_BIG(c.subject_id) events
	--, present in both target and comparator
	--SUM(
	--    (CASE -- comparator cohort start date before target start date (offset)
	--		  WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date)
	--		  THEN datediff(dd, 
	--		                CASE --min of comparator start date/target start dates (offset)
	--			                WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date) 
	--			                THEN c.cohort_start_date
	--			                ELSE DATEADD(day, @start_day_offset, t.cohort_start_date)
	--			              END, 
	--		                CASE --min of comparator end date/target start dates (offset)
	--			                WHEN c.cohort_end_date < DATEADD(day, @start_day_offset, t.cohort_start_date) 
	--			                THEN c.cohort_end_date
	--			                ELSE DATEADD(day, @start_day_offset, t.cohort_start_date)
	--			              END)
	--			ELSE 0
	--			END) + 1) c_days_before_ts
	--,-- comparator cohort days before target start date (offset)
	--
	--SUM(
	--    (CASE -- comparator cohort start date before target end date (offset)
	--		  WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_end_date)
	--		  THEN datediff(dd, 
	--		                CASE --min of comparator start date/target end dates (offset)
	--			                WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_end_date) 
	--			                THEN c.cohort_start_date
	--			                ELSE DATEADD(day, @start_day_offset, t.cohort_end_date)
	--			              END, 
	--		                CASE --min of comparator end date/target end dates (offset)
	--			                WHEN c.cohort_end_date < DATEADD(day, @start_day_offset, t.cohort_end_date) 
	--			                THEN c.cohort_end_date
	--			                ELSE DATEADD(day, @start_day_offset, t.cohort_end_date)
	--			              END)
	--			ELSE 0
	--			END) + 1) c_days_before_te
	--,-- comparator cohort days before target end date (offset)
	--
	--SUM(
	--    (CASE -- comparator cohort days within target days (offset)
	--		  WHEN c.cohort_end_date >= DATEADD(day, @start_day_offset, t.cohort_start_date)
	--		        AND c.cohort_start_date <= DATEADD(day, @end_day_offset, t.cohort_end_date)
	--		  THEN datediff(dd, 
	--		                CASE --max of comparator start date/target start dates (offset)
	--			                WHEN c.cohort_start_date < DATEADD(day, @start_day_offset, t.cohort_start_date) 
	--			                THEN DATEADD(day, @start_day_offset, t.cohort_start_date)
	--			                ELSE c.cohort_start_date
	--			              END, 
	--		                CASE --max of comparator end date/target end dates (offset)
	--			                WHEN c.cohort_end_date < DATEADD(day, @end_day_offset, t.cohort_start_date) 
	--			                THEN DATEADD(day, @end_day_offset, t.cohort_start_date)
	--			                ELSE c.cohort_end_date
	--			              END)
	--			ELSE 0
	--			END) + 1) c_days_within_t_days
	--,-- comparator cohort days within target cohort days (offset)
	--
	--SUM(datediff(dd,
	--            DATEADD(day, @start_day_offset, t.cohort_start_date),
	--            DATEADD(day, @end_day_offset, t.cohort_end_date)) + 1) t_days
	--,-- target cohort days (offset)
	--
	--SUM(datediff(dd,
	--            c.cohort_start_date,
	--            c.cohort_end_date) 
	--            + 1) c_days
	--,-- comparator cohort days (offset)
	--