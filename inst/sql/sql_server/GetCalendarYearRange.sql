{DEFAULT @cdm_database_schema = CDM_jmdc_v1063.dbo}

SELECT YEAR(MIN(observation_period_start_date)) AS start_year,
	YEAR(MAX(observation_period_end_date)) AS end_year
FROM @cdm_database_schema.observation_period;
