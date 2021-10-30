-- Drop old tables if exist

DROP TABLE IF EXISTS analysis_ref;
DROP TABLE IF EXISTS cohort;
DROP TABLE IF EXISTS cohort_count;
DROP TABLE IF EXISTS cohort_inclusion;
DROP TABLE IF EXISTS cohort_inclusion_result;
DROP TABLE IF EXISTS cohort_inclusion_stats;
DROP TABLE IF EXISTS cohort_relationships;
DROP TABLE IF EXISTS concept;
DROP TABLE IF EXISTS concept_ancestor;
DROP TABLE IF EXISTS concept_class;
DROP TABLE IF EXISTS concept_count;
DROP TABLE IF EXISTS concept_cooccurrence;
DROP TABLE IF EXISTS concept_excluded;
DROP TABLE IF EXISTS concept_mapping;
DROP TABLE IF EXISTS concept_relationship;
DROP TABLE IF EXISTS concept_resolved;
DROP TABLE IF EXISTS cohort_summary_stats;
DROP TABLE IF EXISTS concept_sets;
DROP TABLE IF EXISTS concept_sets_optimized;
DROP TABLE IF EXISTS concept_synonym;
DROP TABLE IF EXISTS covariate_ref;
DROP TABLE IF EXISTS covariate_value;
DROP TABLE IF EXISTS covariate_value_dist;
DROP TABLE IF EXISTS database;
DROP TABLE IF EXISTS domain;
DROP TABLE IF EXISTS incidence_rate;
DROP TABLE IF EXISTS inclusion_rule_stats;
DROP TABLE IF EXISTS index_event_breakdown;
DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS orphan_concept;
DROP TABLE IF EXISTS time_series;
DROP TABLE IF EXISTS relationship;
DROP TABLE IF EXISTS temporal_analysis_ref;
DROP TABLE IF EXISTS temporal_covariate_ref;
DROP TABLE IF EXISTS temporal_covariate_value;
DROP TABLE IF EXISTS temporal_covariate_value_dist;
DROP TABLE IF EXISTS temporal_time_ref;
DROP TABLE IF EXISTS time_distribution;
DROP TABLE IF EXISTS visit_context;
DROP TABLE IF EXISTS vocabulary;


-- Create tables
--Table analysis_ref
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE analysis_ref (
			analysis_id BIGINT NOT NULL,
			analysis_name VARCHAR NOT NULL,
			domain_id VARCHAR(20),
			is_binary VARCHAR(1) NOT NULL,
			missing_means_zero VARCHAR(1),
			PRIMARY KEY(analysis_id)
);

--Table cohort
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort (
			cohort_id BIGINT NOT NULL,
			cohort_name VARCHAR(255) NOT NULL,
			metadata VARCHAR,
			sql VARCHAR NOT NULL,
			json VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id)
);


--Table cohort_count
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort_count (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			cohort_entries FLOAT NOT NULL,
			cohort_subjects FLOAT NOT NULL,
			PRIMARY KEY(database_id, cohort_id)
);

--Table cohort_inclusion
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort_inclusion (
	    database_id VARCHAR NOT NULL,
			cohort_id  BIGINT NOT NULL,
			rule_sequence int NOT NULL,
			name varchar NULL,
			description varchar NULL,
			PRIMARY KEY(database_id, cohort_id, rule_sequence)
);

--Table cohort_inclusion_result
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort_inclusion_result (
	database_id VARCHAR NOT NULL,
  cohort_id BIGINT NOT NULL,
  mode_id int NOT NULL,
  inclusion_rule_mask bigint NOT NULL,
  person_count bigint NOT NULL,
	PRIMARY KEY(database_id, cohort_id, inclusion_rule_mask, mode_id)
);

--Table cohort_inclusion_stats
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort_inclusion_stats (
	database_id VARCHAR NOT NULL,
  cohort_id BIGINT NOT NULL,
  rule_sequence int NOT NULL,
  mode_id int NOT NULL,
  person_count bigint NOT NULL,
  gain_count bigint NOT NULL,
  person_total bigint NOT NULL,
	PRIMARY KEY(database_id, cohort_id, rule_sequence, mode_id)
);

--Table cohort_summary_stats
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort_summary_stats(
	database_id VARCHAR NOT NULL,
  cohort_id BIGINT NOT NULL,
  mode_id int NOT NULL,
  base_count bigint NOT NULL,
  final_count bigint NOT NULL,
	PRIMARY KEY(cohort_id, database_id, mode_id)
);


--Table cohort_relationships
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cohort_relationships (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			comparator_cohort_id BIGINT NOT NULL,
			start_day FLOAT NOT NULL,
			end_day FLOAT NOT NULL,
			subjects BIGINT NOT NULL,
			sub_cs_before_ts BIGINT NOT NULL,
			rec_cs_before_ts BIGINT NOT NULL,
			sub_cs_on_ts BIGINT NOT NULL,
			rec_cs_on_ts BIGINT NOT NULL,
			sub_cs_after_ts BIGINT NOT NULL,
			rec_cs_after_ts BIGINT NOT NULL,
			sub_cs_before_te BIGINT NOT NULL,
			rec_cs_before_te BIGINT NOT NULL,
			sub_cs_on_te BIGINT NOT NULL,
			rec_cs_on_te BIGINT NOT NULL,
			sub_cs_after_te BIGINT NOT NULL,
			rec_cs_after_te BIGINT NOT NULL,
			sub_cs_window_t BIGINT NOT NULL,
			rec_cs_window_t BIGINT NOT NULL,
			sub_ce_window_t BIGINT NOT NULL,
			rec_ce_window_t BIGINT NOT NULL,
			sub_cs_window_ts BIGINT NOT NULL,
			rec_cs_window_ts BIGINT NOT NULL,
			sub_cs_window_te BIGINT NOT NULL,
			rec_cs_window_te BIGINT NOT NULL,
			sub_ce_window_ts BIGINT NOT NULL,
			rec_ce_window_ts BIGINT NOT NULL,
			sub_ce_window_te BIGINT NOT NULL,
			rec_ce_window_te BIGINT NOT NULL,
			sub_c_within_t BIGINT NOT NULL,
			rec_c_within_t BIGINT NOT NULL,
			c_days_before_ts BIGINT NOT NULL,
			c_days_before_te BIGINT NOT NULL,
			c_days_within_t_days BIGINT NOT NULL,
			c_days_after_ts BIGINT NOT NULL,
			c_days_after_te BIGINT NOT NULL,
			t_days BIGINT NOT NULL,
			c_days BIGINT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, comparator_cohort_id, start_day, end_day)
);

--Table concept
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept (
			concept_id INT NOT NULL,
			concept_name VARCHAR(255) NOT NULL,
			domain_id VARCHAR(20) NOT NULL,
			vocabulary_id VARCHAR NOT NULL,
			concept_class_id VARCHAR(20) NOT NULL,
			standard_concept VARCHAR(1),
			concept_code VARCHAR NOT NULL,
			valid_start_date DATE NOT NULL,
			valid_end_date DATE NOT NULL,
			invalid_reason VARCHAR,
			PRIMARY KEY(concept_id)
);

--Table concept_ancestor
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_ancestor (
			ancestor_concept_id BIGINT NOT NULL,
			descendant_concept_id BIGINT NOT NULL,
			min_levels_of_separation INT NOT NULL,
			max_levels_of_separation INT NOT NULL,
			PRIMARY KEY(ancestor_concept_id, descendant_concept_id)
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_class (
  concept_class_id			VARCHAR		NOT NULL,
  concept_class_name		VARCHAR	NOT NULL,
  concept_class_concept_id	INTEGER			NOT NULL
);

--Table concept_count
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_count (
			database_id VARCHAR NOT NULL,
			concept_id INT NOT NULL,
			event_year INT NOT NULL,
			event_month INT NOT NULL,
			concept_count FLOAT NOT NULL,
			subject_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, concept_id, event_year, event_month)
);


--Table concept_mapping
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_mapping (
			database_id VARCHAR NOT NULL,
			domain_table VARCHAR NOT NULL,
			concept_id INT NOT NULL,
			source_concept_id INT NOT NULL,
			concept_count FLOAT NOT NULL,
			subject_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, domain_table, concept_id, source_concept_id)
);


--Table concept_relationship
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_relationship (
			concept_id_1 INT NOT NULL,
			concept_id_2 INT NOT NULL,
			relationship_id VARCHAR(20) NOT NULL,
			valid_start_date DATE NOT NULL,
			valid_end_date DATE NOT NULL,
			invalid_reason VARCHAR(1),
			PRIMARY KEY(concept_id_1, concept_id_2, relationship_id)
);

--Table concept_sets
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_sets (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_set_sql VARCHAR NOT NULL,
			concept_set_name VARCHAR(255) NOT NULL,
			concept_set_expression VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id)
);

--Table concept_sets_optimized
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_sets_optimized (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			excluded INT NOT NULL,
			removed INT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, concept_set_id, concept_id, excluded, removed)
);


--Table concept_excluded
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_excluded (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, concept_set_id, concept_id)
);

--Table concept_synonym
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_synonym (
			concept_id INT NOT NULL,
			concept_synonym_name VARCHAR NOT NULL,
			language_concept_id INT NOT NULL,
			PRIMARY KEY(concept_id, concept_synonym_name, language_concept_id)
);

--Table covariate_ref
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE covariate_ref (
			covariate_id BIGINT NOT NULL,
			covariate_name VARCHAR NOT NULL,
			analysis_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(covariate_id)
);

--Table concept_resolved
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_resolved (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id, concept_id, database_id)
);

--Table covariate_value
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE covariate_value (
			cohort_id BIGINT NOT NULL,
			covariate_id BIGINT NOT NULL,
			is_temporal INT NOT NULL,
			start_day FLOAT,
			end_day FLOAT,
			sum_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, covariate_id, , is_temporal, start_day, end_day, database_id)
);

--Table covariate_value_dist
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE covariate_value_dist (
			cohort_id BIGINT NOT NULL,
			covariate_id BIGINT NOT NULL,
			is_temporal INT NOT NULL,
			start_day FLOAT,
			end_day FLOAT,
			count_value FLOAT NOT NULL,
			min_value FLOAT NOT NULL,
			max_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT,
			median_value FLOAT NOT NULL,
			p_10_value FLOAT NOT NULL,
			p_25_value FLOAT NOT NULL,
			p_75_value FLOAT NOT NULL,
			p_90_value FLOAT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, covariate_id, is_temporal, start_day, end_day, database_id)
);

--Table database
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE database (
			database_id VARCHAR NOT NULL,
			database_name VARCHAR,
			description VARCHAR,
			is_meta_analysis VARCHAR(1) NOT NULL,
			vocabulary_version VARCHAR,
			vocabulary_version_cdm VARCHAR,
			PRIMARY KEY(database_id)
);

--Table domain
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE domain (
			domain_id VARCHAR(20) NOT NULL,
			domain_name VARCHAR(255) NOT NULL,
			domain_concept_id INT NOT NULL,
			PRIMARY KEY(domain_id)
);

--Table incidence_rate
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE incidence_rate (
			cohort_count FLOAT NOT NULL,
			person_years FLOAT NOT NULL,
			gender VARCHAR,
			age_group VARCHAR,
			calendar_year VARCHAR(4),
			incidence_rate FLOAT NOT NULL,
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(gender, age_group, calendar_year, cohort_id, database_id)
);


--Table inclusion_rule_stats
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE inclusion_rule_stats (
			rule_sequence_id INT NOT NULL,
			rule_name VARCHAR NOT NULL,
			meet_subjects FLOAT NOT NULL,
			gain_subjects FLOAT NOT NULL,
			total_subjects FLOAT NOT NULL,
			remain_subjects FLOAT NOT NULL,
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(rule_sequence_id, cohort_id, database_id)
);

--Table index_event_breakdown
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE index_event_breakdown (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			days_relative_index BIGINT NOT NULL,
			concept_id INT NOT NULL,
			co_concept_id INT NOT NULL,
			concept_count FLOAT NOT NULL,
			subject_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, days_relative_index, concept_id, co_concept_id)
);

--Table metadata
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE metadata (
			database_id VARCHAR NOT NULL,
			start_time VARCHAR NOT NULL,
			variable_field VARCHAR NOT NULL,
			value_field VARCHAR,
			PRIMARY KEY(database_id, start_time, variable_field)
);

--Table orphan_concept
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE orphan_concept (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			database_id VARCHAR NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id, database_id, concept_id)
);

--Table relationship
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE relationship (
			relationship_id VARCHAR(20) NOT NULL,
			relationship_name VARCHAR(255) NOT NULL,
			is_hierarchical VARCHAR(1) NOT NULL,
			defines_ancestry VARCHAR(1) NOT NULL,
			reverse_relationship_id VARCHAR(20) NOT NULL,
			relationship_concept_id INT NOT NULL,
			PRIMARY KEY(relationship_id, reverse_relationship_id, relationship_concept_id)
);


--Table temporal_analysis_ref
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE temporal_analysis_ref (
			analysis_id INT NOT NULL,
			analysis_name VARCHAR NOT NULL,
			domain_id VARCHAR(20) NOT NULL,
			is_binary VARCHAR(1) NOT NULL,
			missing_means_zero VARCHAR(1),
			PRIMARY KEY(analysis_id, domain_id)
);

--Table temporal_covariate_ref
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE temporal_covariate_ref (
			covariate_id BIGINT NOT NULL,
			covariate_name VARCHAR NOT NULL,
			analysis_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(covariate_id)
);

--Table temporal_covariate_value
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE temporal_covariate_value (
			cohort_id BIGINT NOT NULL,
			start_day FLOAT,
			end_day FLOAT,
			covariate_id BIGINT NOT NULL,
			sum_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, covariate_id, start_day, end_day, database_id)
);

--Table temporal_covariate_value_dist
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE temporal_covariate_value_dist (
			cohort_id BIGINT NOT NULL,
			time_id INT NOT NULL,
			covariate_id BIGINT NOT NULL,
			count_value FLOAT NOT NULL,
			min_value FLOAT NOT NULL,
			max_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT NOT NULL,
			median_value FLOAT NOT NULL,
			p_10_value FLOAT NOT NULL,
			p_25_value FLOAT NOT NULL,
			p_75_value FLOAT NOT NULL,
			p_90_value FLOAT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, covariate_id, start_day, end_day, database_id)
);

--Table temporal_time_ref
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE temporal_time_ref (
			time_id INT NOT NULL,
			start_day FLOAT NOT NULL,
			end_day FLOAT NOT NULL,
			PRIMARY KEY(time_id)
);

--Table time_distribution
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE time_distribution (
			cohort_id BIGINT NOT NULL,
			count_value FLOAT NOT NULL,
			min_value FLOAT NOT NULL,
			max_value FLOAT NOT NULL,
			average_value FLOAT NOT NULL,
			standard_deviation FLOAT NOT NULL,
			median_value FLOAT NOT NULL,
			p_10_value FLOAT NOT NULL,
			p_25_value FLOAT NOT NULL,
			p_75_value FLOAT NOT NULL,
			p_90_value FLOAT NOT NULL,
			time_metric VARCHAR NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, time_metric, database_id)
);

--Table time_series
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE time_series (
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			period_begin DATE NOT NULL,
			calendar_interval VARCHAR NOT NULL,
			series_type VARCHAR NOT NULL,
			records BIGINT NOT NULL,
			subjects BIGINT NOT NULL,
			person_days BIGINT NOT NULL,
			person_days_in BIGINT NOT NULL,
			records_start BIGINT,
			subjects_start BIGINT,
			subjects_start_in BIGINT,
			records_end BIGINT,
			subjects_end BIGINT,
			subjects_end_in BIGINT,
			PRIMARY KEY(cohort_id, database_id, period_begin, calendar_interval, series_type)
);

--Table visit_context
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE visit_context (
			cohort_id BIGINT NOT NULL,
			visit_concept_id INT NOT NULL,
			visit_concept_name VARCHAR NOT NULL,
			visit_context VARCHAR NOT NULL,
			subjects FLOAT NOT NULL,
			records FLOAT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, visit_concept_id, visit_context, database_id)
);

--Table vocabulary
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE vocabulary (
			vocabulary_id VARCHAR NOT NULL,
			vocabulary_name VARCHAR(255) NOT NULL,
			vocabulary_reference VARCHAR,
			vocabulary_version VARCHAR,
			vocabulary_concept_id INT NOT NULL,
			PRIMARY KEY(vocabulary_id)
);
