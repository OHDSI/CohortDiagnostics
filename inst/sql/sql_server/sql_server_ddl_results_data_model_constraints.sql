--DDL Primary Key Constraints Specification for package CohortDiagnostics package version: 2.0
 --Data Model Version 2.0
 --Last update 2020-09-25
 --Number of tables 27
 
 ALTER TABLE @resultsDatabaseSchema.analysis_ref ADD CONSTRAINT xpk_analysis_ref PRIMARY KEY NONCLUSTERED (analysis_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort ADD CONSTRAINT xpk_cohort PRIMARY KEY NONCLUSTERED (cohort_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort_count ADD CONSTRAINT xpk_cohort_count PRIMARY KEY NONCLUSTERED (cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort_overlap ADD CONSTRAINT xpk_cohort_overlap PRIMARY KEY NONCLUSTERED (target_cohort_id,comparator_cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.concept ADD CONSTRAINT xpk_concept PRIMARY KEY NONCLUSTERED (concept_id,domain_id,vocabulary_id);  
 ALTER TABLE @resultsDatabaseSchema.concept_ancestor ADD CONSTRAINT xpk_concept_ancestor PRIMARY KEY NONCLUSTERED (ancestor_concept_id,descendant_concept_id);  
 ALTER TABLE @resultsDatabaseSchema.concept_relationship ADD CONSTRAINT xpk_concept_relationship PRIMARY KEY NONCLUSTERED (concept_id_1,concept_id_2,relationship_id);  
 ALTER TABLE @resultsDatabaseSchema.concept_sets ADD CONSTRAINT xpk_concept_sets PRIMARY KEY NONCLUSTERED (cohort_id,concept_set_id);  
 ALTER TABLE @resultsDatabaseSchema.concept_synonym ADD CONSTRAINT xpk_concept_synonym PRIMARY KEY NONCLUSTERED (concept_id,concept_synonym_name,language_concept_id);  
 ALTER TABLE @resultsDatabaseSchema.covariate_ref ADD CONSTRAINT xpk_covariate_ref PRIMARY KEY NONCLUSTERED (covariate_id);  
 ALTER TABLE @resultsDatabaseSchema.covariate_value ADD CONSTRAINT xpk_covariate_value PRIMARY KEY NONCLUSTERED (cohort_id,covariate_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.database ADD CONSTRAINT xpk_database PRIMARY KEY NONCLUSTERED (database_id);  
 ALTER TABLE @resultsDatabaseSchema.domain ADD CONSTRAINT xpk_domain PRIMARY KEY NONCLUSTERED (domain_id);  
 ALTER TABLE @resultsDatabaseSchema.incidence_rate ADD CONSTRAINT xpk_incidence_rate PRIMARY KEY NONCLUSTERED (gender,age_group,calendar_year,cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.included_source_concept ADD CONSTRAINT xpk_included_source_concept PRIMARY KEY NONCLUSTERED (database_id,cohort_id,concept_set_id,concept_id,source_concept_id);  
 ALTER TABLE @resultsDatabaseSchema.inclusion_rule_stats ADD CONSTRAINT xpk_inclusion_rule_stats PRIMARY KEY NONCLUSTERED (rule_sequence_id,cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.index_event_breakdown ADD CONSTRAINT xpk_index_event_breakdown PRIMARY KEY NONCLUSTERED (concept_id,cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.orphan_concept ADD CONSTRAINT xpk_orphan_concept PRIMARY KEY NONCLUSTERED (cohort_id,concept_set_id,database_id,concept_id);  
 ALTER TABLE @resultsDatabaseSchema.phenotype_description ADD CONSTRAINT xpk_phenotype_description PRIMARY KEY NONCLUSTERED (phenotype_id);  
 ALTER TABLE @resultsDatabaseSchema.relationship ADD CONSTRAINT xpk_relationship PRIMARY KEY NONCLUSTERED (relationship_id,reverse_relationship_id,relationship_concept_id);  
 ALTER TABLE @resultsDatabaseSchema.temporal_analysis_ref ADD CONSTRAINT xpk_temporal_analysis_ref PRIMARY KEY NONCLUSTERED (analysis_id,domain_id);  
 ALTER TABLE @resultsDatabaseSchema.temporal_covariate_ref ADD CONSTRAINT xpk_temporal_covariate_ref PRIMARY KEY NONCLUSTERED (covariate_id);  
 ALTER TABLE @resultsDatabaseSchema.temporal_covariate_value ADD CONSTRAINT xpk_temporal_covariate_value PRIMARY KEY NONCLUSTERED (cohort_id,time_id,covariate_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.temporal_time_ref ADD CONSTRAINT xpk_temporal_time_ref PRIMARY KEY NONCLUSTERED (time_id);  
 ALTER TABLE @resultsDatabaseSchema.time_distribution ADD CONSTRAINT xpk_time_distribution PRIMARY KEY NONCLUSTERED (cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.visit_context ADD CONSTRAINT xpk_visit_context PRIMARY KEY NONCLUSTERED (cohort_id,visit_concept_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.vocabulary ADD CONSTRAINT xpk_vocabulary PRIMARY KEY NONCLUSTERED (vocabulary_id,vocabulary_concept_id); 
