SELECT domain_name,
  concept.concept_id,
  concept.concept_name,
  CASE 
	WHEN type_concept.concept_id = 0 THEN CAST('' AS VARCHAR(255))
	ELSE type_concept.concept_name 
  END AS type_concept_name,
  start_date,
  end_date
FROM (
{@drug_era} ? {
  SELECT drug_era_start_date AS start_date,
    drug_era_end_date AS end_date,
    drug_concept_id AS concept_id,
	CAST(0 AS INT) AS type_concept_id,
    CAST('Drug era' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.drug_era
  WHERE drug_era.person_id = @subject_id
}
{@drug_exposure} ? {
{@drug_era} ? {
  UNION ALL
}
  SELECT drug_exposure_start_date AS start_date,
    drug_exposure_end_date AS end_date,
    drug_concept_id AS concept_id,
	drug_type_concept_id AS type_concept_id,
    CAST('Drug exposure' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.drug_exposure
  WHERE drug_exposure.person_id = @subject_id
}
{@condition_era} ? {
{@drug_era | @drug_exposure} ? {
  UNION ALL
}
  SELECT condition_era_start_date AS start_date,
    condition_era_end_date AS end_date,
    condition_concept_id AS concept_id,
	CAST(0 AS INT) AS type_concept_id,
    CAST('Condition era' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.condition_era
  WHERE condition_era.person_id = @subject_id
}
{@condition_occurrence} ? {
{@drug_era | @drug_exposure | @condition_era} ? {
  UNION ALL
}
  SELECT condition_start_date AS start_date,
    condition_end_date AS end_date,
    condition_concept_id AS concept_id,
	condition_type_concept_id AS type_concept_id,
    CAST('Condition occurrence' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.condition_occurrence
  WHERE condition_occurrence.person_id = @subject_id
}
{@procedure} ? {
{@drug_era | @drug_exposure | @condition_era | @condition_occurrence} ? {
  UNION ALL
}
  SELECT procedure_date AS start_date,
    procedure_date AS end_date,
    procedure_concept_id AS concept_id,
	procedure_type_concept_id AS type_concept_id,
    CAST('Procedure' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.procedure_occurrence
  WHERE procedure_occurrence.person_id = @subject_id
}
{@visit} ? {
{@drug_era | @drug_exposure | @condition_era | @condition_occurrence | @procedure} ? {
  UNION ALL
}
  SELECT visit_start_date AS start_date,
    visit_end_date AS end_date,
    visit_concept_id AS concept_id,
	visit_type_concept_id AS type_concept_id,
    CAST('Visit' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.visit_occurrence
  WHERE visit_occurrence.person_id = @subject_id
}
{@observation_period} ? {
{@drug_era | @drug_exposure | @condition_era | @condition_occurrence | @procedure | @visit} ? {
  UNION ALL
}
  SELECT observation_period_start_date AS start_date,
    observation_period_end_date AS end_date,
    CAST(0 AS INT) AS concept_id,
	period_type_concept_id AS type_concept_id,
    CAST('Observation period' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.observation_period
  WHERE observation_period.person_id = @subject_id
}
{@observation} ? {
{@drug_era | @drug_exposure | @condition_era | @condition_occurrence | @procedure | @visit | @observation_period} ? {
  UNION ALL
}
  SELECT observation_date AS start_date,
    observation_date AS end_date,
    observation_concept_id AS concept_id,
	observation_type_concept_id AS type_concept_id,
    CAST('Observation' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.observation
  WHERE observation.person_id = @subject_id
}
{@measurement} ? {
{@drug_era | @drug_exposure | @condition_era | @condition_occurrence | @procedure | @visit | @observation_period | @observation} ? {
  UNION ALL
}
  SELECT measurement_date AS start_date,
    measurement_date AS end_date,
    measurement_concept_id AS concept_id,
	measurement_type_concept_id AS type_concept_id,
    CAST('Measurement' AS VARCHAR(255)) AS domain_name
  FROM @cdm_database_schema.measurement
  WHERE measurement.person_id = @subject_id
}
  ) all_events
INNER JOIN @vocabulary_database_schema.concept concept
  ON all_events.concept_id = concept.concept_id
INNER JOIN @vocabulary_database_schema.concept type_concept
  ON all_events.type_concept_id = type_concept.concept_id;
