-- Drop old tables if exist

DROP TABLE IF EXISTS analysis_ref;
DROP TABLE IF EXISTS calendar_incidence;
DROP TABLE IF EXISTS cohort;
DROP TABLE IF EXISTS cohort_count;
DROP TABLE IF EXISTS cohort_inclusion;
DROP TABLE IF EXISTS cohort_inclusion_result;
DROP TABLE IF EXISTS cohort_inclusion_stats;
DROP TABLE IF EXISTS cohort_summary_stats;
DROP TABLE IF EXISTS cohort_relationships;
DROP TABLE IF EXISTS concept;
DROP TABLE IF EXISTS concept_ancestor;
DROP TABLE IF EXISTS concept_relationship;
DROP TABLE IF EXISTS concept_sets;
DROP TABLE IF EXISTS concept_synonym;
DROP TABLE IF EXISTS covariate_ref;
DROP TABLE IF EXISTS covariate_value;
DROP TABLE IF EXISTS covariate_value_dist;
DROP TABLE IF EXISTS database;
DROP TABLE IF EXISTS domain;
DROP TABLE IF EXISTS incidence_rate;
DROP TABLE IF EXISTS included_source_concept;
DROP TABLE IF EXISTS inclusion_rule_stats;
DROP TABLE IF EXISTS index_event_breakdown;
DROP TABLE IF EXISTS metadata;
DROP TABLE IF EXISTS orphan_concept;
DROP TABLE IF EXISTS time_series;
DROP TABLE IF EXISTS relationship;
DROP TABLE IF EXISTS resolved_concepts;
DROP TABLE IF EXISTS temporal_analysis_ref;
DROP TABLE IF EXISTS temporal_covariate_ref;
DROP TABLE IF EXISTS temporal_covariate_value;
DROP TABLE IF EXISTS temporal_covariate_dist;
DROP TABLE IF EXISTS temporal_time_ref;
DROP TABLE IF EXISTS time_distribution;
DROP TABLE IF EXISTS visit_context;
DROP TABLE IF EXISTS vocabulary;


-- Create tables

--Table analysis_ref

CREATE TABLE analysis_ref (
			analysis_id BIGINT NOT NULL,
			analysis_name VARCHAR NOT NULL,
			domain_id VARCHAR(20),
			start_day FLOAT,
			end_day FLOAT,
			is_binary VARCHAR(1) NOT NULL,
			missing_means_zero VARCHAR(1),
			PRIMARY KEY(analysis_id)
);

--Table calendar_incidence

CREATE TABLE calendar_incidence (
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			period_type VARCHAR NOT NULL,
			calendar_month DATE NOT NULL,
			count_value FLOAT,
			PRIMARY KEY(cohort_id, database_id, period_type, calendar_month)
);


--Table cohort

CREATE TABLE cohort (
			cohort_id BIGINT NOT NULL,
			web_api_cohort_id BIGINT NOT NULL,
			cohort_name VARCHAR(255) NOT NULL,
			logic_description VARCHAR,
			metadata VARCHAR,
			sql VARCHAR NOT NULL,
			json VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id)
);


--Table cohort_count

CREATE TABLE cohort_count (
			cohort_id BIGINT NOT NULL,
			cohort_entries FLOAT NOT NULL,
			cohort_subjects FLOAT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, database_id)
);

--Table cohort_inclusion

CREATE TABLE cohort_inclusion (
			cohort_id  BIGINT NOT NULL,
			rule_sequence int NOT NULL,
			name varchar NULL,
			description varchar NULL,
	    database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, rule_sequence, database_id)
);

--Table cohort_inclusion_result

CREATE TABLE cohort_inclusion_result (
  cohort_id BIGINT NOT NULL,
  mode_id int NOT NULL,
  inclusion_rule_mask bigint NOT NULL,
  person_count bigint NOT NULL,
	database_id VARCHAR NOT NULL,
	PRIMARY KEY(cohort_id, database_id, inclusion_rule_mask, mode_id)
);

--Table cohort_inclusion_stats

CREATE TABLE cohort_inclusion_stats (
  cohort_id BIGINT NOT NULL,
  rule_sequence int NOT NULL,
  mode_id int NOT NULL,
  person_count bigint NOT NULL,
  gain_count bigint NOT NULL,
  person_total bigint NOT NULL,
	database_id VARCHAR NOT NULL,
	PRIMARY KEY(cohort_id, database_id, rule_sequence, mode_id)
);

--Table cohort_summary_stats

CREATE TABLE cohort_summary_stats(
  cohort_id BIGINT NOT NULL,
  mode_id int NOT NULL,
  base_count bigint NOT NULL,
  final_count bigint NOT NULL,
	database_id VARCHAR NOT NULL,
	PRIMARY KEY(cohort_id, database_id, mode_id)
);


--Table cohort_relationships

CREATE TABLE cohort_relationships (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			comparator_cohort_id BIGINT NOT NULL,
			start_day FLOAT NOT NULL,
			end_day FLOAT NOT NULL,
			target_records BIGINT NOT NULL,
			target_subjects BIGINT NOT NULL,
			comparator_records BIGINT NOT NULL,
			comparator_subjects BIGINT NOT NULL,
			both_records BIGINT NOT NULL,
			both_subjects BIGINT NOT NULL,
			t_records_only BIGINT NOT NULL,
			t_subjects_only BIGINT NOT NULL,
			c_before_t_records BIGINT NOT NULL,
			c_before_t_subjects BIGINT NOT NULL,
			t_before_c_records BIGINT NOT NULL,
			t_before_c_subjects BIGINT NOT NULL,
			same_day_records BIGINT NOT NULL,
			same_day_subjects BIGINT NOT NULL,
			c_person_days BIGINT NOT NULL,
			c_records_start BIGINT NOT NULL,
			c_subjects_start BIGINT NOT NULL,
			c_records_end BIGINT NOT NULL,
			c_subjects_end BIGINT NOT NULL,
			c_in_t_records BIGINT NOT NULL,
			c_in_t_subjects BIGINT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, comparator_cohort_id, start_day, end_day)
);

--Table concept

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

CREATE TABLE concept_ancestor (
			ancestor_concept_id BIGINT NOT NULL,
			descendant_concept_id BIGINT NOT NULL,
			min_levels_of_separation INT NOT NULL,
			max_levels_of_separation INT NOT NULL,
			PRIMARY KEY(ancestor_concept_id, descendant_concept_id)
);

--Table concept_relationship

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

CREATE TABLE concept_sets (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_set_sql VARCHAR NOT NULL,
			concept_set_name VARCHAR(255) NOT NULL,
			concept_set_expression VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id)
);

--Table concept_synonym

CREATE TABLE concept_synonym (
			concept_id INT NOT NULL,
			concept_synonym_name VARCHAR NOT NULL,
			language_concept_id INT NOT NULL,
			PRIMARY KEY(concept_id, concept_synonym_name, language_concept_id)
);

--Table covariate_ref

CREATE TABLE covariate_ref (
			covariate_id BIGINT NOT NULL,
			covariate_name VARCHAR NOT NULL,
			analysis_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(covariate_id)
);

--Table covariate_value

CREATE TABLE covariate_value (
			cohort_id BIGINT NOT NULL,
			covariate_id BIGINT NOT NULL,
			sum_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, covariate_id, database_id)
);

--Table covariate_value_dist

CREATE TABLE covariate_value_dist (
			cohort_id BIGINT NOT NULL,
			covariate_id BIGINT NOT NULL,
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
			PRIMARY KEY(cohort_id, covariate_id, database_id)
);

--Table database

CREATE TABLE database (
			database_id VARCHAR NOT NULL,
			database_name VARCHAR,
			description VARCHAR,
			is_meta_analysis VARCHAR(1) NOT NULL,
			observation_period_min_date DATE NOT NULL,
			observation_period_max_date DATE NOT NULL,
			persons BIGINT,
			records BIGINT,
			person_days BIGINT,
			vocabulary_version VARCHAR,
			vocabulary_version_cdm VARCHAR,
			PRIMARY KEY(database_id)
);

--Table domain

CREATE TABLE domain (
			domain_id VARCHAR(20) NOT NULL,
			domain_name VARCHAR(255) NOT NULL,
			domain_concept_id INT NOT NULL,
			PRIMARY KEY(domain_id)
);

--Table incidence_rate

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

--Table included_source_concept

CREATE TABLE included_source_concept (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			source_concept_id INT NOT NULL,
			concept_subjects FLOAT NOT NULL,
			concept_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, concept_set_id, concept_id, source_concept_id)
);

--Table inclusion_rule_stats

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

CREATE TABLE index_event_breakdown (
			concept_id INT NOT NULL,
			concept_count FLOAT NOT NULL,
			subject_count FLOAT NOT NULL,
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			domain_field VARCHAR NOT NULL,
			domain_table VARCHAR NOT NULL,
			PRIMARY KEY(concept_id, cohort_id, database_id, domain_field, domain_table)
);

--Table metadata

CREATE TABLE metadata (
			database_id VARCHAR NOT NULL,
			start_time VARCHAR NOT NULL,
			variable_field VARCHAR NOT NULL,
			value_field VARCHAR,
			PRIMARY KEY(database_id, start_time, variable_field)
);

--Table orphan_concept

CREATE TABLE orphan_concept (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			database_id VARCHAR NOT NULL,
			concept_id INT NOT NULL,
			concept_count FLOAT NOT NULL,
			concept_subjects FLOAT NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id, database_id, concept_id)
);

--Table relationship

CREATE TABLE relationship (
			relationship_id VARCHAR(20) NOT NULL,
			relationship_name VARCHAR(255) NOT NULL,
			is_hierarchical VARCHAR(1) NOT NULL,
			defines_ancestry VARCHAR(1) NOT NULL,
			reverse_relationship_id VARCHAR(20) NOT NULL,
			relationship_concept_id INT NOT NULL,
			PRIMARY KEY(relationship_id, reverse_relationship_id, relationship_concept_id)
);

--Table resolved_concepts

CREATE TABLE resolved_concepts (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id, concept_id, database_id)
);

--Table temporal_analysis_ref

CREATE TABLE temporal_analysis_ref (
			analysis_id INT NOT NULL,
			analysis_name VARCHAR NOT NULL,
			domain_id VARCHAR(20) NOT NULL,
			is_binary VARCHAR(1) NOT NULL,
			missing_means_zero VARCHAR(1),
			PRIMARY KEY(analysis_id, domain_id)
);

--Table temporal_covariate_ref

CREATE TABLE temporal_covariate_ref (
			covariate_id BIGINT NOT NULL,
			covariate_name VARCHAR NOT NULL,
			analysis_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(covariate_id)
);

--Table temporal_covariate_value

CREATE TABLE temporal_covariate_value (
			cohort_id BIGINT NOT NULL,
			time_id INT NOT NULL,
			covariate_id BIGINT NOT NULL,
			sum_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, time_id, covariate_id, database_id)
);

--Table temporal_covariate_dist

CREATE TABLE temporal_covariate_dist (
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
			PRIMARY KEY(cohort_id, covariate_id, database_id)
);

--Table temporal_time_ref

CREATE TABLE temporal_time_ref (
			time_id INT NOT NULL,
			start_day FLOAT NOT NULL,
			end_day FLOAT NOT NULL,
			PRIMARY KEY(time_id)
);

--Table time_distribution

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

CREATE TABLE time_series (
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			period_begin DATE NOT NULL,
			calendar_interval VARCHAR NOT NULL,
			series_type VARCHAR NOT NULL,
			records BIGINT NOT NULL,
			subjects BIGINT NOT NULL,
			person_days BIGINT NOT NULL,
			records_start BIGINT,
			subjects_start BIGINT,
			records_end BIGINT,
			subjects_end BIGINT,
			PRIMARY KEY(cohort_id, database_id, period_begin, calendar_interval, series_type)
);

--Table visit_context

CREATE TABLE visit_context (
			cohort_id BIGINT NOT NULL,
			visit_concept_id INT NOT NULL,
			visit_concept_NAME VARCHAR NOT NULL,
			visit_context VARCHAR NOT NULL,
			subjects FLOAT NOT NULL,
			records FLOAT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, visit_concept_id, visit_context, database_id)
);

--Table vocabulary

CREATE TABLE vocabulary (
			vocabulary_id VARCHAR NOT NULL,
			vocabulary_name VARCHAR(255) NOT NULL,
			vocabulary_reference VARCHAR,
			vocabulary_version VARCHAR,
			vocabulary_concept_id INT NOT NULL,
			PRIMARY KEY(vocabulary_id)
);
