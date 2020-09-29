--DDL Drop table Specification for package CohortDiagnostics package version: 2.0
 --Data Model Version 2.0
 --Last update 2020-09-25
 --Number of tables 27
 
 DROP TABLE IF EXISTS @resultsDatabaseSchema.analysis_ref;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_count;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_overlap;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.concept;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.concept_ancestor;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.concept_relationship;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.concept_sets;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.concept_synonym;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.covariate_ref;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.covariate_value;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.database;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.domain;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.incidence_rate;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.included_source_concept;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.inclusion_rule_stats;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.index_event_breakdown;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.orphan_concept;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.phenotype_description;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.relationship;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.temporal_analysis_ref;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.temporal_covariate_ref;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.temporal_covariate_value;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.temporal_time_ref;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.time_distribution;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.visit_context;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.vocabulary; 
