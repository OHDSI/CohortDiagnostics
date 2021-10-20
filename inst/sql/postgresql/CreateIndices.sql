CREATE INDEX idx_concept_concept_id  ON concept  (concept_id ASC);
CLUSTER concept  USING idx_concept_concept_id ;
CREATE INDEX idx_concept_code ON concept (concept_code ASC);
CREATE INDEX idx_concept_vocabluary_id ON concept (vocabulary_id ASC);
CREATE INDEX idx_concept_domain_id ON concept (domain_id ASC);
CREATE INDEX idx_concept_class_id ON concept (concept_class_id ASC);

CREATE INDEX idx_vocabulary_vocabulary_id  ON vocabulary  (vocabulary_id ASC);
CLUSTER vocabulary  USING idx_vocabulary_vocabulary_id ;

CREATE INDEX idx_domain_domain_id  ON domain  (domain_id ASC);
CLUSTER domain  USING idx_domain_domain_id ;

CREATE INDEX idx_concept_class_class_id  ON concept_class  (concept_class_id ASC);
CLUSTER concept_class  USING idx_concept_class_class_id ;

CREATE INDEX idx_concept_relationship_id_1  ON concept_relationship  (concept_id_1 ASC);
CLUSTER concept_relationship  USING idx_concept_relationship_id_1 ;
CREATE INDEX idx_concept_relationship_id_2 ON concept_relationship (concept_id_2 ASC);
CREATE INDEX idx_concept_relationship_id_3 ON concept_relationship (relationship_id ASC);

CREATE INDEX idx_relationship_rel_id  ON relationship  (relationship_id ASC);
CLUSTER relationship  USING idx_relationship_rel_id ;

CREATE INDEX idx_concept_synonym_id  ON concept_synonym  (concept_id ASC);
CLUSTER concept_synonym  USING idx_concept_synonym_id ;

CREATE INDEX idx_concept_ancestor_id_1  ON concept_ancestor  (ancestor_concept_id ASC);
CLUSTER concept_ancestor  USING idx_concept_ancestor_id_1 ;
CREATE INDEX idx_concept_ancestor_id_2 ON concept_ancestor (descendant_concept_id ASC);

--------------------Non Vocabulary ------------------


CREATE INDEX idx_analysis_id  ON analysis_ref  (analysis_id ASC);
CLUSTER analysis_ref  USING idx_analysis_id ;
CREATE INDEX idx_analysis_name  ON analysis_ref  (analysis_name ASC);

CREATE INDEX idx_cohort_id_1  ON cohort  (cohort_id ASC);
CLUSTER cohort  USING idx_cohort_id_1 ;
CREATE INDEX idx_cohort_name  ON cohort  (cohort_name ASC);

CREATE INDEX idx_cohort_id_2  ON cohort_count  (cohort_id ASC);
CLUSTER cohort_count  USING idx_cohort_id_2 ;
CREATE INDEX idx_database_id_2  ON cohort_count  (database_id ASC);

CREATE INDEX idx_cohort_id_3  ON cohort_inclusion  (cohort_id ASC);
CLUSTER cohort_inclusion  USING idx_cohort_id_3 ;
CREATE INDEX idx_database_id_3  ON cohort_inclusion  (database_id ASC);
CREATE INDEX idx_rule_sequence_3  ON cohort_inclusion  (rule_sequence ASC);

CREATE INDEX idx_cohort_id_4  ON cohort_inclusion_result  (cohort_id ASC);
CLUSTER cohort_inclusion_result  USING idx_cohort_id_4 ;
CREATE INDEX idx_database_id_4  ON cohort_inclusion_result  (database_id ASC);
CREATE INDEX idx_mode_id_4  ON cohort_inclusion_result  (mode_id ASC);

CREATE INDEX idx_cohort_id_5  ON cohort_inclusion_stats  (cohort_id ASC);
CLUSTER cohort_inclusion_stats  USING idx_cohort_id_5 ;
CREATE INDEX idx_database_id_5  ON cohort_inclusion_stats  (database_id ASC);
CREATE INDEX idx_rule_sequence_5  ON cohort_inclusion_stats  (rule_sequence ASC);
CREATE INDEX idx_mode_id_5  ON cohort_inclusion_stats  (mode_id ASC);

CREATE INDEX idx_cohort_id_6  ON cohort_summary_stats  (cohort_id ASC);
CLUSTER cohort_summary_stats  USING idx_cohort_id_6 ;
CREATE INDEX idx_database_id_6  ON cohort_summary_stats  (database_id ASC);
CREATE INDEX idx_mode_id_6  ON cohort_summary_stats  (mode_id ASC);

CREATE INDEX idx_cohort_id_7  ON cohort_relationships  (cohort_id ASC);
CLUSTER cohort_relationships  USING idx_cohort_id_7 ;
CREATE INDEX idx_database_id_7  ON cohort_relationships  (database_id ASC);
CREATE INDEX idx_comparator_cohort_id_7  ON cohort_relationships  (comparator_cohort_id ASC);
CREATE INDEX idx_start_day_7  ON cohort_relationships  (start_day ASC);
CREATE INDEX idx_end_day_7  ON cohort_relationships  (end_day ASC);

CREATE INDEX idx_concept_id_8  ON concept_count  (concept_id ASC);
CLUSTER concept_count  USING idx_concept_id_8 ;
CREATE INDEX idx_database_id_8  ON concept_count  (database_id ASC);
CREATE INDEX idx_event_year_8  ON concept_count  (event_year ASC);
CREATE INDEX idx_event_month_8  ON concept_count  (event_month ASC);

CREATE INDEX idx_concept_id_9  ON concept_mapping  (concept_id ASC);
CLUSTER concept_mapping  USING idx_concept_id_9 ;
CREATE INDEX idx_source_concept_id_9  ON concept_mapping  (source_concept_id ASC);
CREATE INDEX idx_domain_table_9  ON concept_mapping  (domain_table ASC);

CREATE INDEX idx_cohort_id_10  ON concept_sets  (cohort_id ASC);
CLUSTER concept_sets  USING idx_cohort_id_10 ;
CREATE INDEX idx_concept_set_id_10  ON concept_sets  (concept_set_id ASC);
CREATE INDEX idx_concept_set_name_10  ON concept_sets  (concept_set_name ASC);

CREATE INDEX idx_cohort_id_11  ON concept_sets_optimized  (cohort_id ASC);
CLUSTER concept_sets_optimized  USING idx_cohort_id_11 ;
CREATE INDEX idx_database_id_11  ON concept_sets_optimized  (database_id ASC);
CREATE INDEX idx_concept_set_id_11  ON concept_sets_optimized  (concept_set_id ASC);

CREATE INDEX idx_cohort_id_12  ON concept_excluded  (cohort_id ASC);
CLUSTER concept_excluded  USING idx_cohort_id_12 ;
CREATE INDEX idx_database_id_12  ON concept_excluded  (database_id ASC);
CREATE INDEX idx_concept_set_id_12  ON concept_excluded  (concept_set_id ASC);
CREATE INDEX idx_concept_id_12  ON concept_excluded  (concept_id ASC);

CREATE INDEX idx_cohort_id_13  ON concept_resolved  (cohort_id ASC);
CLUSTER concept_resolved  USING idx_cohort_id_13 ;
CREATE INDEX idx_database_id_13  ON concept_resolved  (database_id ASC);
CREATE INDEX idx_concept_set_id_13  ON concept_resolved  (concept_set_id ASC);
CREATE INDEX idx_concept_id_13  ON concept_resolved  (concept_id ASC);

CREATE INDEX idx_cohort_id_14  ON covariate_value  (cohort_id ASC);
CLUSTER covariate_value  USING idx_cohort_id_14 ;
CREATE INDEX idx_database_id_14  ON covariate_value  (database_id ASC);
CREATE INDEX idx_covariate_id_14  ON covariate_value  (covariate_id ASC);

CREATE INDEX idx_cohort_id_15  ON covariate_value_dist  (cohort_id ASC);
CLUSTER covariate_value_dist  USING idx_cohort_id_15 ;
CREATE INDEX idx_database_id_15  ON covariate_value_dist  (database_id ASC);
CREATE INDEX idx_covariate_id_15  ON covariate_value_dist  (covariate_id ASC);

CREATE INDEX idx_database_id_16  ON database  (database_id ASC);
CLUSTER database  USING idx_database_id_16 ;
CREATE INDEX idx_database_name_16  ON database  (database_name ASC);

CREATE INDEX idx_cohort_id_17  ON incidence_rate  (cohort_id ASC);
CLUSTER incidence_rate  USING idx_cohort_id_17 ;
CREATE INDEX idx_database_id_17  ON incidence_rate  (database_id ASC);
CREATE INDEX idx_gender_17  ON incidence_rate  (gender ASC);
CREATE INDEX idx_age_group_17  ON incidence_rate  (age_group ASC);
CREATE INDEX idx_calendar_year_17  ON incidence_rate  (calendar_year ASC);

CREATE INDEX idx_cohort_id_18  ON inclusion_rule_stats  (cohort_id ASC);
CLUSTER inclusion_rule_stats  USING idx_cohort_id_18 ;
CREATE INDEX idx_database_id_18  ON inclusion_rule_stats  (database_id ASC);
CREATE INDEX idx_rule_sequence_id_18  ON inclusion_rule_stats  (rule_sequence_id ASC);
CREATE INDEX idx_rule_name_18  ON inclusion_rule_stats  (rule_name ASC);

CREATE INDEX idx_cohort_id_19  ON index_event_breakdown  (cohort_id ASC);
CLUSTER index_event_breakdown  USING idx_cohort_id_19 ;
CREATE INDEX idx_database_id_19  ON index_event_breakdown  (database_id ASC);
CREATE INDEX idx_days_relative_index_19  ON index_event_breakdown  (days_relative_index ASC);
CREATE INDEX idx_concept_id_19  ON index_event_breakdown  (concept_id ASC);
CREATE INDEX idx_co_concept_id_19  ON index_event_breakdown  (co_concept_id ASC);

CREATE INDEX idx_database_id_20  ON metadata  (database_id ASC);
CLUSTER metadata  USING idx_database_id_20 ;
CREATE INDEX idx_start_time_20  ON metadata  (start_time ASC);
CREATE INDEX idx_variable_field_20  ON metadata  (variable_field ASC);

CREATE INDEX idx_cohort_id_21  ON orphan_concept  (cohort_id ASC);
CLUSTER orphan_concept  USING idx_cohort_id_21 ;
CREATE INDEX idx_database_id_21  ON orphan_concept  (database_id ASC);
CREATE INDEX idx_concept_set_id_21  ON orphan_concept  (concept_set_id ASC);
CREATE INDEX idx_concept_id_21  ON orphan_concept  (concept_id ASC);

CREATE INDEX idx_analysis_id_22  ON temporal_analysis_ref  (analysis_id ASC);
CLUSTER temporal_analysis_ref  USING idx_analysis_id_22 ;
CREATE INDEX idx_analysis_name_22  ON temporal_analysis_ref  (analysis_name ASC);
CREATE INDEX idx_domain_id_22  ON temporal_analysis_ref  (domain_id ASC);

CREATE INDEX idx_covariate_id_23  ON temporal_covariate_ref  (covariate_id ASC);
CLUSTER temporal_covariate_ref  USING idx_analysis_id_23 ;
CREATE INDEX idx_analysis_name_23  ON temporal_covariate_ref  (analysis_name ASC);
CREATE INDEX idx_covariate_name_23  ON temporal_covariate_ref  (covariate_name ASC);
CREATE INDEX idx_concept_id_23  ON temporal_covariate_ref  (concept_id ASC);

CREATE INDEX idx_cohort_id_24  ON temporal_covariate_value  (cohort_id ASC);
CLUSTER temporal_covariate_value  USING idx_cohort_id_24 ;
CREATE INDEX idx_database_id_24  ON temporal_covariate_value  (database_id ASC);
CREATE INDEX idx_time_id_24  ON temporal_covariate_value  (time_id ASC);
CREATE INDEX idx_covariate_id_24  ON temporal_covariate_value  (covariate_id ASC);

CREATE INDEX idx_cohort_id_25  ON temporal_covariate_value_dist  (cohort_id ASC);
CLUSTER temporal_covariate_value_dist  USING idx_cohort_id_25 ;
CREATE INDEX idx_database_id_25  ON temporal_covariate_value_dist  (database_id ASC);
CREATE INDEX idx_time_id_25  ON temporal_covariate_value_dist  (time_id ASC);
CREATE INDEX idx_covariate_id_25  ON temporal_covariate_value_dist  (covariate_id ASC);

CREATE INDEX idx_time_id_26  ON temporal_time_ref  (time_id ASC);
CLUSTER temporal_time_ref  USING idx_time_id_26 ;
CREATE INDEX idx_start_day_26  ON temporal_time_ref  (start_day ASC);
CREATE INDEX idx_end_day_26  ON temporal_time_ref  (end_day ASC);

CREATE INDEX idx_cohort_id_27  ON time_distribution  (cohort_id ASC);
CLUSTER time_distribution  USING idx_cohort_id_27 ;
CREATE INDEX idx_database_id_27  ON time_distribution  (database_id ASC);
CREATE INDEX idx_time_metric_27  ON time_distribution  (time_metric ASC);

CREATE INDEX idx_cohort_id_28  ON time_series  (cohort_id ASC);
CLUSTER time_series  USING idx_cohort_id_28 ;
CREATE INDEX idx_database_id_28  ON time_series  (database_id ASC);
CREATE INDEX idx_period_begin_28  ON time_series  (period_begin ASC);
CREATE INDEX idx_calendar_interval_28  ON time_series  (calendar_interval ASC);
CREATE INDEX idx_series_type_28  ON time_series  (series_type ASC);

CREATE INDEX idx_cohort_id_28  ON visit_context  (cohort_id ASC);
CLUSTER visit_context  USING idx_cohort_id_28 ;
CREATE INDEX idx_database_id_28  ON visit_context  (database_id ASC);
CREATE INDEX idx_visit_concept_id_28  ON visit_context  (visit_concept_id ASC);
CREATE INDEX idx_visit_concept_name_28  ON visit_context  (visit_concept_name ASC);
CREATE INDEX idx_visit_context_28  ON visit_context  (visit_context ASC);
