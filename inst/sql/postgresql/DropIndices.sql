DROP INDEX IF EXISTS idx_concept_concept_id ;
DROP INDEX IF EXISTS idx_concept_code;
DROP INDEX IF EXISTS idx_concept_vocabluary_id;
DROP INDEX IF EXISTS idx_concept_domain_id;
DROP INDEX IF EXISTS idx_concept_class_id;

DROP INDEX IF EXISTS idx_vocabulary_vocabulary_id;

DROP INDEX IF EXISTS idx_domain_domain_id;

DROP INDEX IF EXISTS idx_concept_class_class_id;

DROP INDEX IF EXISTS idx_concept_relationship_id_1;
DROP INDEX IF EXISTS idx_concept_relationship_id_2;
DROP INDEX IF EXISTS idx_concept_relationship_id_3;

DROP INDEX IF EXISTS idx_relationship_rel_id;

DROP INDEX IF EXISTS idx_concept_synonym_id;

DROP INDEX IF EXISTS idx_concept_ancestor_id_1;
DROP INDEX IF EXISTS idx_concept_ancestor_id_2;

--------------------Non Vocabulary ------------------


DROP INDEX IF EXISTS idx_analysis_id;
DROP INDEX IF EXISTS idx_analysis_name;

DROP INDEX IF EXISTS idx_cohort_id_1;
DROP INDEX IF EXISTS idx_cohort_name;

DROP INDEX IF EXISTS idx_cohort_id_2;
DROP INDEX IF EXISTS idx_database_id_2;

DROP INDEX IF EXISTS idx_cohort_id_3;
DROP INDEX IF EXISTS idx_database_id_3;
DROP INDEX IF EXISTS idx_rule_sequence_3;

DROP INDEX IF EXISTS idx_cohort_id_4;
DROP INDEX IF EXISTS idx_database_id_4;
DROP INDEX IF EXISTS idx_mode_id_4;

DROP INDEX IF EXISTS idx_cohort_id_5;
DROP INDEX IF EXISTS idx_database_id_5;
DROP INDEX IF EXISTS idx_rule_sequence_5;
DROP INDEX IF EXISTS idx_mode_id_5;

DROP INDEX IF EXISTS idx_cohort_id_6;
DROP INDEX IF EXISTS idx_database_id_6;
DROP INDEX IF EXISTS idx_mode_id_6;

DROP INDEX IF EXISTS idx_cohort_id_7;
DROP INDEX IF EXISTS idx_database_id_7;
DROP INDEX IF EXISTS idx_comparator_cohort_id_7;
DROP INDEX IF EXISTS idx_start_day_7;
DROP INDEX IF EXISTS idx_end_day_7;

DROP INDEX IF EXISTS idx_concept_id_8;
DROP INDEX IF EXISTS idx_database_id_8;
DROP INDEX IF EXISTS idx_event_year_8;
DROP INDEX IF EXISTS idx_event_month_8;

DROP INDEX IF EXISTS idx_concept_id_9;
DROP INDEX IF EXISTS idx_source_concept_id_9;
DROP INDEX IF EXISTS idx_domain_table_9;

DROP INDEX IF EXISTS idx_cohort_id_10;
DROP INDEX IF EXISTS idx_concept_set_id_10;
DROP INDEX IF EXISTS idx_concept_set_name_10;

DROP INDEX IF EXISTS idx_cohort_id_11;
DROP INDEX IF EXISTS idx_database_id_11;
DROP INDEX IF EXISTS idx_concept_set_id_11;

DROP INDEX IF EXISTS idx_cohort_id_12;
DROP INDEX IF EXISTS idx_database_id_12;
DROP INDEX IF EXISTS idx_concept_set_id_12;
DROP INDEX IF EXISTS idx_concept_id_12;

DROP INDEX IF EXISTS idx_cohort_id_13;
DROP INDEX IF EXISTS idx_database_id_13;
DROP INDEX IF EXISTS idx_concept_set_id_13;
DROP INDEX IF EXISTS idx_concept_id_13;

DROP INDEX IF EXISTS idx_cohort_id_14;
DROP INDEX IF EXISTS idx_database_id_14;
DROP INDEX IF EXISTS idx_covariate_id_14;

DROP INDEX IF EXISTS idx_cohort_id_15;
DROP INDEX IF EXISTS idx_database_id_15;
DROP INDEX IF EXISTS idx_covariate_id_15;

DROP INDEX IF EXISTS idx_database_id_16;
DROP INDEX IF EXISTS idx_database_name_16;

DROP INDEX IF EXISTS idx_cohort_id_17;
DROP INDEX IF EXISTS idx_database_id_17;
DROP INDEX IF EXISTS idx_gender_17;
DROP INDEX IF EXISTS idx_age_group_17;
DROP INDEX IF EXISTS idx_calendar_year_17;

DROP INDEX IF EXISTS idx_cohort_id_18;
DROP INDEX IF EXISTS idx_database_id_18;
DROP INDEX IF EXISTS idx_rule_sequence_id_18;
DROP INDEX IF EXISTS idx_rule_name_18;

DROP INDEX IF EXISTS idx_cohort_id_19;
DROP INDEX IF EXISTS idx_database_id_19;
DROP INDEX IF EXISTS idx_days_relative_index_19;
DROP INDEX IF EXISTS idx_concept_id_19;
DROP INDEX IF EXISTS idx_co_concept_id_19;

DROP INDEX IF EXISTS idx_database_id_20;
DROP INDEX IF EXISTS idx_start_time_20;
DROP INDEX IF EXISTS idx_variable_field_20;

DROP INDEX IF EXISTS idx_cohort_id_21;
DROP INDEX IF EXISTS idx_database_id_21;
DROP INDEX IF EXISTS idx_concept_set_id_21;
DROP INDEX IF EXISTS idx_concept_id_21;

DROP INDEX IF EXISTS idx_analysis_id_22;
DROP INDEX IF EXISTS idx_analysis_name_22;
DROP INDEX IF EXISTS idx_domain_id_22;

DROP INDEX IF EXISTS idx_covariate_id_23;
DROP INDEX IF EXISTS idx_analysis_name_23;
DROP INDEX IF EXISTS idx_covariate_name_23;
DROP INDEX IF EXISTS idx_concept_id_23;

DROP INDEX IF EXISTS idx_cohort_id_24;
DROP INDEX IF EXISTS idx_database_id_24;
DROP INDEX IF EXISTS idx_time_id_24;
DROP INDEX IF EXISTS idx_covariate_id_24;

DROP INDEX IF EXISTS idx_cohort_id_25;
DROP INDEX IF EXISTS idx_database_id_25;
DROP INDEX IF EXISTS idx_time_id_25;
DROP INDEX IF EXISTS idx_covariate_id_25;

DROP INDEX IF EXISTS idx_time_id_26;
DROP INDEX IF EXISTS idx_start_day_26;
DROP INDEX IF EXISTS idx_end_day_26;

DROP INDEX IF EXISTS idx_cohort_id_27;
DROP INDEX IF EXISTS idx_database_id_27;
DROP INDEX IF EXISTS idx_time_metric_27;

DROP INDEX IF EXISTS idx_cohort_id_28;
DROP INDEX IF EXISTS idx_database_id_28;
DROP INDEX IF EXISTS idx_period_begin_28;
DROP INDEX IF EXISTS idx_calendar_interval_28;
DROP INDEX IF EXISTS idx_series_type_28;

DROP INDEX IF EXISTS idx_cohort_id_28;
DROP INDEX IF EXISTS idx_database_id_28;
DROP INDEX IF EXISTS idx_visit_concept_id_28;
DROP INDEX IF EXISTS idx_visit_concept_name_28;
DROP INDEX IF EXISTS idx_visit_context_28;
