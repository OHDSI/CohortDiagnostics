{DEFAULT @analysis_ref = analysis_ref}
{DEFAULT @annotation = annotation}
{DEFAULT @annotation_link = annotation_link}
{DEFAULT @annotation_attributes = annotation_attributes}
{DEFAULT @cohort = cohort}
{DEFAULT @cohort_count = cohort_count}
{DEFAULT @cohort_overlap = cohort_overlap}
{DEFAULT @cohort_relationships = cohort_relationships}
{DEFAULT @concept = concept}
{DEFAULT @concept_ancestor = concept_ancestor}
{DEFAULT @concept_class = concept_class}
{DEFAULT @concept_count = concept_count}
{DEFAULT @concept_excluded = concept_excluded}
{DEFAULT @concept_std_src_cnt = concept_std_src_cnt}
{DEFAULT @concept_optimized = concept_optimized}
{DEFAULT @concept_relationship = concept_relationship}
{DEFAULT @concept_resolved = concept_resolved}
{DEFAULT @concept_sets = concept_sets}
{DEFAULT @concept_synonym = concept_synonym}
{DEFAULT @covariate_ref = covariate_ref}
{DEFAULT @covariate_value = covariate_value}
{DEFAULT @covariate_value_dist = covariate_value_dist}
{DEFAULT @database = database}
{DEFAULT @domain = domain}
{DEFAULT @incidence_rate = incidence_rate}
{DEFAULT @included_source_concept = included_source_concept}
{DEFAULT @inclusion_rule_stats = inclusion_rule_stats}
{DEFAULT @index_event_breakdown = index_event_breakdown}
{DEFAULT @metadata = metadata}
{DEFAULT @orphan_concept = orphan_concept}
{DEFAULT @relationship = relationship}
{DEFAULT @resolved_concepts = resolved_concepts}
{DEFAULT @temporal_analysis_ref = temporal_analysis_ref}
{DEFAULT @temporal_covariate_ref = temporal_covariate_ref}
{DEFAULT @temporal_covariate_value = temporal_covariate_value}
{DEFAULT @temporal_covariate_value_dist = temporal_covariate_value_dist}
{DEFAULT @temporal_time_ref = temporal_time_ref}
{DEFAULT @time_distribution = time_distribution}
{DEFAULT @time_series = time_series}
{DEFAULT @visit_context = visit_context}
{DEFAULT @vocabulary = vocabulary}


-- Drop old tables if exist
DROP TABLE IF EXISTS @results_schema.@analysis_ref;
DROP TABLE IF EXISTS @results_schema.@annotation;
DROP TABLE IF EXISTS @results_schema.@annotation_link;
DROP TABLE IF EXISTS @results_schema.@annotation_attributes;
DROP TABLE IF EXISTS @results_schema.@cohort;
DROP TABLE IF EXISTS @results_schema.@cohort_count;
DROP TABLE IF EXISTS @results_schema.@cohort_overlap;
DROP TABLE IF EXISTS @results_schema.@cohort_relationships;
DROP TABLE IF EXISTS @results_schema.@concept;
DROP TABLE IF EXISTS @results_schema.@concept_ancestor;
DROP TABLE IF EXISTS @results_schema.@concept_relationship;
DROP TABLE IF EXISTS @results_schema.@concept_sets;
DROP TABLE IF EXISTS @results_schema.@concept_synonym;
DROP TABLE IF EXISTS @results_schema.@covariate_ref;
DROP TABLE IF EXISTS @results_schema.@covariate_value;
DROP TABLE IF EXISTS @results_schema.@covariate_value_dist;
DROP TABLE IF EXISTS @results_schema.@database;
DROP TABLE IF EXISTS @results_schema.@domain;
DROP TABLE IF EXISTS @results_schema.@incidence_rate;
DROP TABLE IF EXISTS @results_schema.@included_source_concept;
DROP TABLE IF EXISTS @results_schema.@inclusion_rule_stats;
DROP TABLE IF EXISTS @results_schema.@index_event_breakdown;
DROP TABLE IF EXISTS @results_schema.@metadata;
DROP TABLE IF EXISTS @results_schema.@orphan_concept;
DROP TABLE IF EXISTS @results_schema.@relationship;
DROP TABLE IF EXISTS @results_schema.@resolved_concepts;
DROP TABLE IF EXISTS @results_schema.@temporal_analysis_ref;
DROP TABLE IF EXISTS @results_schema.@temporal_covariate_ref;
DROP TABLE IF EXISTS @results_schema.@temporal_covariate_value;
DROP TABLE IF EXISTS @results_schema.@temporal_covariate_value_dist;
DROP TABLE IF EXISTS @results_schema.@temporal_time_ref;
DROP TABLE IF EXISTS @results_schema.@time_distribution;
DROP TABLE IF EXISTS @results_schema.@time_series;
DROP TABLE IF EXISTS @results_schema.@visit_context;
DROP TABLE IF EXISTS @results_schema.@vocabulary;



-- Create tables

--Table analysis_ref

CREATE TABLE @results_schema.@analysis_ref (
			analysis_id BIGINT NOT NULL,
			analysis_name VARCHAR NOT NULL,
			domain_id VARCHAR(20),
			start_day FLOAT,
			end_day FLOAT,
			is_binary VARCHAR(1) NOT NULL,
			missing_means_zero VARCHAR(1),
			PRIMARY KEY(analysis_id)
);

--Table annotation

CREATE TABLE @results_schema.@annotation (
      annotation_id BIGINT NOT NULL,
			created_by VARCHAR NOT NULL,
			created_on BIGINT NOT NULL,
			modified_last_on BIGINT,
			deleted_on BIGINT,
			annotation VARCHAR NOT NULL,
			PRIMARY KEY(annotation_id)
);

--Table annotation_link

CREATE TABLE @results_schema.@annotation_link (
      annotation_id BIGINT NOT NULL DEFAULT 0,
			diagnostics_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(annotation_id, diagnostics_id, cohort_id, database_id)
);

--Table annotation_attributes

CREATE TABLE @results_schema.@annotation_attributes (
      annotation_id BIGINT NOT NULL DEFAULT 0,
			created_by VARCHAR NOT NULL,
			annotation_attributes VARCHAR NOT NULL,
			created_on BIGINT NOT NULL,
			PRIMARY KEY(annotation_id, created_by)
);

--Table cohort

CREATE TABLE @results_schema.@cohort (
			cohort_id BIGINT NOT NULL,
			cohort_name VARCHAR(255) NOT NULL,
			metadata VARCHAR,
			sql VARCHAR NOT NULL,
			json VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id)
);


--Table cohort_count

CREATE TABLE @results_schema.@cohort_count (
			cohort_id BIGINT NOT NULL,
			cohort_entries FLOAT NOT NULL,
			cohort_subjects FLOAT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, database_id)
);

--Table cohort_overlap

CREATE TABLE @results_schema.@cohort_overlap (
			either_subjects FLOAT NOT NULL,
			both_subjects FLOAT NOT NULL,
			t_only_subjects FLOAT NOT NULL,
			c_only_subjects FLOAT NOT NULL,
			t_before_c_subjects FLOAT NOT NULL,
			c_before_t_subjects FLOAT NOT NULL,
			same_day_subjects FLOAT NOT NULL,
			t_in_c_subjects FLOAT NOT NULL,
			c_in_t_subjects FLOAT NOT NULL,
			target_cohort_id BIGINT NOT NULL,
			comparator_cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(target_cohort_id, comparator_cohort_id, database_id)
);


--Table cohort_relationships
CREATE TABLE @results_schema.@cohort_relationships (
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

CREATE TABLE @results_schema.@concept (
			concept_id BIGINT NOT NULL,
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

CREATE TABLE @results_schema.@concept_ancestor (
			ancestor_concept_id BIGINT NOT NULL,
			descendant_concept_id BIGINT NOT NULL,
			min_levels_of_separation INT NOT NULL,
			max_levels_of_separation INT NOT NULL,
			PRIMARY KEY(ancestor_concept_id, descendant_concept_id)
);

--Table concept_class

CREATE TABLE @results_schema.@concept_class (
  concept_class_id			VARCHAR		NOT NULL,
  concept_class_name		VARCHAR	NOT NULL,
  concept_class_concept_id	INTEGER			NOT NULL
);

--Table concept_count

CREATE TABLE @results_schema.@concept_count (
			database_id VARCHAR NOT NULL,
			concept_id INT NOT NULL,
			event_year INT NOT NULL,
			event_month INT NOT NULL,
			concept_count FLOAT NOT NULL,
			subject_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, concept_id, event_year, event_month)
);

--Table concept_excluded

CREATE TABLE @results_schema.@concept_excluded (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, concept_set_id, concept_id)
);

--Table concept_sets_optimized

CREATE TABLE @results_schema.@concept_sets_optimized (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			excluded INT NOT NULL,
			removed INT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, concept_set_id, concept_id, excluded, removed)
);

--Table concept_std_src_cnt

CREATE TABLE @results_schema.@concept_std_src_cnt (
			database_id VARCHAR NOT NULL,
			domain_table VARCHAR NOT NULL,
			concept_id INT NOT NULL,
			source_concept_id INT NOT NULL,
			concept_count FLOAT NOT NULL,
			subject_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, domain_table, concept_id, source_concept_id)
);

--Table concept_relationship

CREATE TABLE @results_schema.@concept_relationship (
			concept_id_1 BIGINT NOT NULL,
			concept_id_2 BIGINT NOT NULL,
			relationship_id VARCHAR(20) NOT NULL,
			valid_start_date DATE NOT NULL,
			valid_end_date DATE NOT NULL,
			invalid_reason VARCHAR(1),
			PRIMARY KEY(concept_id_1, concept_id_2, relationship_id)
);

--Table concept_resolved

CREATE TABLE @results_schema.@concept_resolved (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id INT NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id, concept_id, database_id)
);

--Table concept_sets

CREATE TABLE @results_schema.@concept_sets (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_set_sql VARCHAR NOT NULL,
			concept_set_name VARCHAR(255) NOT NULL,
			concept_set_expression VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id)
);

--Table concept_synonym

CREATE TABLE @results_schema.@concept_synonym (
			concept_id BIGINT NOT NULL,
			concept_synonym_name VARCHAR NOT NULL,
			language_concept_id BIGINT NOT NULL,
			PRIMARY KEY(concept_id, concept_synonym_name, language_concept_id)
);

--Table covariate_ref

CREATE TABLE @results_schema.@covariate_ref (
			covariate_id BIGINT NOT NULL,
			covariate_name VARCHAR NOT NULL,
			analysis_id INT NOT NULL,
			concept_id BIGINT NOT NULL,
			PRIMARY KEY(covariate_id)
);

--Table covariate_value

CREATE TABLE @results_schema.@covariate_value (
			cohort_id BIGINT NOT NULL,
			covariate_id BIGINT NOT NULL,
			sum_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, covariate_id, database_id)
);

--Table covariate_value_dist

CREATE TABLE @results_schema.@covariate_value_dist (
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

CREATE TABLE @results_schema.@database (
			database_id VARCHAR NOT NULL,
			database_name VARCHAR,
			description VARCHAR,
			is_meta_analysis VARCHAR(1) NOT NULL,
			vocabulary_version VARCHAR,
			vocabulary_version_cdm VARCHAR,
			PRIMARY KEY(database_id)
);

--Table domain

CREATE TABLE @results_schema.@domain (
			domain_id VARCHAR(20) NOT NULL,
			domain_name VARCHAR(255) NOT NULL,
			domain_concept_id BIGINT NOT NULL,
			PRIMARY KEY(domain_id)
);

--Table incidence_rate

CREATE TABLE @results_schema.@incidence_rate (
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

CREATE TABLE @results_schema.@included_source_concept (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id BIGINT NOT NULL,
			source_concept_id BIGINT NOT NULL,
			concept_subjects FLOAT NOT NULL,
			concept_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, concept_set_id, concept_id, source_concept_id)
);

--Table inclusion_rule_stats

CREATE TABLE @results_schema.@inclusion_rule_stats (
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

CREATE TABLE @results_schema.@index_event_breakdown (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			days_relative_index BIGINT NOT NULL DEFAULT 0,
			concept_id INT NOT NULL,
			co_concept_id INT NOT NULL,
			concept_count FLOAT NOT NULL,
			subject_count FLOAT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, days_relative_index, concept_id, co_concept_id)
);

--Table metadata

CREATE TABLE @results_schema.@metadata (
			database_id VARCHAR NOT NULL,
			start_time VARCHAR DEFAULT '0:0:0',
			variable_field VARCHAR NOT NULL,
			value_field VARCHAR,
			PRIMARY KEY(database_id, start_time, variable_field)
);

--Table orphan_concept

CREATE TABLE @results_schema.@orphan_concept (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			database_id VARCHAR NOT NULL,
			concept_id BIGINT NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id, database_id, concept_id)
);

--Table relationship

CREATE TABLE @results_schema.@relationship (
			relationship_id VARCHAR(20) NOT NULL,
			relationship_name VARCHAR(255) NOT NULL,
			is_hierarchical VARCHAR(1) NOT NULL,
			defines_ancestry VARCHAR(1) NOT NULL,
			reverse_relationship_id VARCHAR(20) NOT NULL,
			relationship_concept_id BIGINT NOT NULL,
			PRIMARY KEY(relationship_id, reverse_relationship_id, relationship_concept_id)
);

--Table resolved_concepts

CREATE TABLE @results_schema.@resolved_concepts (
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, concept_set_id, concept_id, database_id)
);

--Table temporal_analysis_ref

CREATE TABLE @results_schema.@temporal_analysis_ref (
			analysis_id INT NOT NULL,
			analysis_name VARCHAR NOT NULL,
			domain_id VARCHAR(20) NOT NULL,
			is_binary VARCHAR(1) NOT NULL,
			missing_means_zero VARCHAR(1),
			PRIMARY KEY(analysis_id, domain_id)
);

--Table temporal_covariate_ref

CREATE TABLE @results_schema.@temporal_covariate_ref (
			covariate_id BIGINT NOT NULL,
			covariate_name VARCHAR NOT NULL,
			analysis_id INT NOT NULL,
			concept_id BIGINT NOT NULL,
			PRIMARY KEY(covariate_id)
);

--Table temporal_covariate_value

CREATE TABLE @results_schema.@temporal_covariate_value (
			cohort_id BIGINT NOT NULL,
			time_id INT NOT NULL,
			covariate_id BIGINT NOT NULL,
			sum_value FLOAT NOT NULL,
			mean FLOAT NOT NULL,
			sd FLOAT,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, time_id, covariate_id, database_id)
);

--Table temporal_covariate_value_dist

CREATE TABLE @results_schema.@temporal_covariate_value_dist (
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

CREATE TABLE @results_schema.@temporal_time_ref (
			time_id INT NOT NULL,
			start_day FLOAT NOT NULL,
			end_day FLOAT NOT NULL,
			PRIMARY KEY(time_id)
);

--Table time_distribution

CREATE TABLE @results_schema.@time_distribution (
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
CREATE TABLE @results_schema.@time_series (
			cohort_id BIGINT NOT NULL,
			database_id VARCHAR NOT NULL,
			period_begin DATE NOT NULL,
			period_end DATE NOT NULL,
			calendar_interval VARCHAR NOT NULL,
			gender VARCHAR,
			age_group VARCHAR,
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
			PRIMARY KEY(cohort_id, database_id, period_begin, period_end, calendar_interval, gender, age_group, series_type)
);

--Table visit_context

CREATE TABLE @results_schema.@visit_context (
			cohort_id BIGINT NOT NULL,
			visit_concept_id BIGINT NOT NULL,
			visit_context VARCHAR NOT NULL,
			subjects FLOAT NOT NULL,
			database_id VARCHAR NOT NULL,
			PRIMARY KEY(cohort_id, visit_concept_id, visit_context, database_id)
);

--Table vocabulary

CREATE TABLE @results_schema.@vocabulary (
			vocabulary_id VARCHAR NOT NULL,
			vocabulary_name VARCHAR(255) NOT NULL,
			vocabulary_reference VARCHAR,
			vocabulary_version VARCHAR,
			vocabulary_concept_id BIGINT NOT NULL
);
