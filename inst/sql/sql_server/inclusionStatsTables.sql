IF OBJECT_ID('tempdb..#cohort_inc_result', 'U') IS NOT NULL 
  DROP TABLE #cohort_inc_result;
  
IF OBJECT_ID('tempdb..#cohort_inc_stats', 'U') IS NOT NULL 
  DROP TABLE #cohort_inc_stats;
  
IF OBJECT_ID('tempdb..#cohort_summary_stats', 'U') IS NOT NULL 
  DROP TABLE #cohort_summary_stats;
  
IF OBJECT_ID('@results_schema.cohort_censor_stats', 'U') IS NOT NULL
  DROP TABLE #cohort_censor_stats;

CREATE TABLE #cohort_inc_result (
	cohort_definition_id BIGINT NOT NULL,
	inclusion_rule_mask BIGINT NOT NULL,
	person_count BIGINT NOT NULL,
	mode_id INT
	);

CREATE TABLE #cohort_inc_stats (
	cohort_definition_id BIGINT NOT NULL,
	rule_sequence INT NOT NULL,
	person_count BIGINT NOT NULL,
	gain_count BIGINT NOT NULL,
	person_total BIGINT NOT NULL,
	mode_id INT
	);

CREATE TABLE #cohort_summary_stats (
	cohort_definition_id BIGINT NOT NULL,
	base_count BIGINT NOT NULL,
	final_count BIGINT NOT NULL,
	mode_id INT
	);

CREATE TABLE #cohort_censor_stats (
  cohort_definition_id int NOT NULL,
  lost_count BIGINT NOT NULL
);
