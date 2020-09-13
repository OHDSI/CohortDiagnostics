--DDL Drop table Specification for package CohortDiagnostics package version: 2.0
 --Data Model Version 2.0
 --Last update 2020-09-13
 --Number of tables 8
 
 DROP TABLE IF EXISTS @resultsDatabaseSchema.analysis_ref;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_count;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_inclusion;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_inclusion_result;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_inclusion_stats;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_overlap;  
 DROP TABLE IF EXISTS @resultsDatabaseSchema.cohort_summary_stats; 