--DDL Primary Key Constraints Specification for package CohortDiagnostics package version: 2.0
 --Data Model Version 2.0
 --Last update 2020-09-13
 --Number of tables 9
 
 ALTER TABLE @resultsDatabaseSchema.analysis_ref ADD CONSTRAINT xpk_analysis_ref PRIMARY KEY NONCLUSTERED (analysis_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort ADD CONSTRAINT xpk_cohort PRIMARY KEY NONCLUSTERED (referent_concept_id,cohort_id,web_api_cohort_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort_count ADD CONSTRAINT xpk_cohort_count PRIMARY KEY NONCLUSTERED (cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort_inclusion ADD CONSTRAINT xpk_cohort_inclusion PRIMARY KEY NONCLUSTERED (cohort_id,rule_sequence);  
 ALTER TABLE @resultsDatabaseSchema.cohort_inclusion_result ADD CONSTRAINT xpk_cohort_inclusion_result PRIMARY KEY NONCLUSTERED (cohort_id,mode_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort_inclusion_stats ADD CONSTRAINT xpk_cohort_inclusion_stats PRIMARY KEY NONCLUSTERED (cohort_id,rule_sequence,mode_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort_overlap ADD CONSTRAINT xpk_cohort_overlap PRIMARY KEY NONCLUSTERED (target_cohort_id,comparator_cohort_id,database_id);  
 ALTER TABLE @resultsDatabaseSchema.cohort_summary_stats ADD CONSTRAINT xpk_cohort_summary_stats PRIMARY KEY NONCLUSTERED (cohort_id,mode_id);  
 ALTER TABLE @resultsDatabaseSchema.concept ADD CONSTRAINT xpk_concept PRIMARY KEY NONCLUSTERED (concept_id,domain_id,vocabulary_id,concept_class_id); 