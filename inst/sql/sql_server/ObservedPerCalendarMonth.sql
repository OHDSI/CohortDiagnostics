SELECT starts.event_year,
	starts.event_month,
	start_count,
	end_count
FROM (
	SELECT YEAR(observation_period_start_date) AS event_year,
		MONTH(observation_period_start_date) AS event_month,
		COUNT(*) AS start_count
	FROM @cdm_database_schema.observation_period
	GROUP BY YEAR(observation_period_start_date),
		MONTH(observation_period_start_date)
	) starts
FULL JOIN (
	SELECT YEAR(observation_period_end_date) AS event_year,
		MONTH(observation_period_end_date) AS event_month,
		COUNT(*) AS end_count
	FROM @cdm_database_schema.observation_period
	GROUP BY YEAR(observation_period_end_date),
		MONTH(observation_period_end_date)
	) ends
	ON starts.event_year = ends.event_year
		AND starts.event_month = ends.event_month
