--DDL Specification for package CohortDiagnostics package version: 2.0
 --Data Model Version 2.0
 --Last update 2020-09-25
 --Number of tables 23
 
 ----------------------------------------------------------------------- 
 --Table name analysis_ref
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.analysis_ref (
 
			analysis_id bigint NOT NULL, 
			analysis_name varchar(50) NOT NULL, 
			domain_id varchar(20) NULL, 
			start_day float NULL, 
			end_day float NULL, 
			is_binary varchar(1) NOT NULL, 
			missing_means_zero varchar(1) NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort (
 
			referent_concept_id bigint NOT NULL, 
			cohort_id bigint NOT NULL, 
			web_api_cohort_id bigint NOT NULL, 
			cohort_name varchar(255) NOT NULL, 
			logic_description varchar(max) NOT NULL, 
			sql varchar(max) NOT NULL, 
			json varchar(max) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_count
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_count (
 
			cohort_id bigint NOT NULL, 
			cohort_entries float NOT NULL, 
			cohort_subjects float NOT NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_overlap
 --Number of fields in table 12
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_overlap (
 
			either_subjects float NOT NULL, 
			both_subjects float NOT NULL, 
			t_only_subjects float NOT NULL, 
			c_only_subjects float NOT NULL, 
			t_before_c_subjects float NOT NULL, 
			c_before_t_subjects float NOT NULL, 
			same_day_subjects float NOT NULL, 
			t_in_c_subjects float NOT NULL, 
			c_in_t_subjects float NOT NULL, 
			target_cohort_id bigint NOT NULL, 
			comparator_cohort_id bigint NOT NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_ancestor
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_ancestor (
 
			ancestor_concept_id bigint NOT NULL, 
			descendant_concept_id bigint NOT NULL, 
			min_levels_of_separation int NOT NULL, 
			max_levels_of_separation int NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_relationship
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_relationship (
 
			concept_id_1 int NOT NULL, 
			concept_id_2 int NOT NULL, 
			relationship_id varchar(20) NOT NULL, 
			valid_start_date Date NOT NULL, 
			valid_end_date Date NOT NULL, 
			invalid_reason varchar(1) NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_sets
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_sets (
 
			cohort_id bigint NOT NULL, 
			concept_set_id int NOT NULL, 
			concept_set_sql varchar(max) NOT NULL, 
			concept_set_name varchar(255) NOT NULL, 
			concept_set_expression varchar(max) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name covariate_ref
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.covariate_ref (
 
			covariate_id bigint NOT NULL, 
			covariate_name varchar(max) NOT NULL, 
			analysis_id int NOT NULL, 
			concept_id int NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name covariate_value
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.covariate_value (
 
			cohort_id bigint NOT NULL, 
			covariate_id bigint NOT NULL, 
			mean float NOT NULL, 
			sd float NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name database
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.database (
 
			database_id varchar(20) NOT NULL, 
			database_name varchar(max) NULL, 
			description varchar(max) NULL, 
			is_meta_analysis varchar(1) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name incidence_rate
 --Number of fields in table 8
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.incidence_rate (
 
			cohort_count float NOT NULL, 
			person_years float NOT NULL, 
			gender varchar(max) NULL, 
			age_group varchar(max) NULL, 
			calendar_year float NULL, 
			incidence_rate float NOT NULL, 
			cohort_id bigint NOT NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name included_source_concept
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.included_source_concept (
 
			database_id varchar(20) NOT NULL, 
			cohort_id bigint NOT NULL, 
			concept_set_id int NOT NULL, 
			concept_id int NOT NULL, 
			source_concept_id int NOT NULL, 
			concept_subjects float NOT NULL, 
			concept_count float NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name inclusion_rule_stats
 --Number of fields in table 8
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.inclusion_rule_stats (
 
			rule_sequence_id int NOT NULL, 
			rule_name varchar(255) NOT NULL, 
			meet_subjects float NOT NULL, 
			gain_subjects float NOT NULL, 
			total_subjects float NOT NULL, 
			remain_subjects float NOT NULL, 
			cohort_id bigint NOT NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name index_event_breakdown
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.index_event_breakdown (
 
			concept_id int NOT NULL, 
			concept_count float NOT NULL, 
			cohort_id bigint NOT NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name orphan_concept
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.orphan_concept (
 
			cohort_id bigint NOT NULL, 
			concept_set_id int NOT NULL, 
			database_id varchar(20) NOT NULL, 
			concept_id int NOT NULL, 
			concept_count float NOT NULL, 
			concept_subjects float NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name phenotype_description
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.phenotype_description (
 
			phenotype_id bigint NOT NULL, 
			phenotype_name varchar(255) NOT NULL, 
			referent_concept_id int NOT NULL, 
			clinical_description varchar(max) NOT NULL, 
			literature_review varchar(max) NULL, 
			phenotype_notes varchar(max) NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name relationship
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.relationship (
 
			relationship_id varchar(20) NOT NULL, 
			relationship_name varchar(255) NOT NULL, 
			is_hierarchical varchar(1) NOT NULL, 
			defines_ancestry varchar(1) NOT NULL, 
			reverse_relationship_id varchar(20) NOT NULL, 
			relationship_concept_id int NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_analysis_ref
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_analysis_ref (
 
			analysis_id int NOT NULL, 
			analysis_name varchar(20) NOT NULL, 
			domain_id varchar(20) NOT NULL, 
			is_binary varchar(1) NOT NULL, 
			missing_means_zero varchar(1) NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_covariate_ref
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_covariate_ref (
 
			covariate_id bigint NOT NULL, 
			covariate_name varchar(max) NOT NULL, 
			analysis_id int NOT NULL, 
			concept_id int NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_covariate_value
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_covariate_value (
 
			cohort_id bigint NOT NULL, 
			time_id int NOT NULL, 
			covariate_id bigint NOT NULL, 
			mean float NOT NULL, 
			sd float NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_time_ref
 --Number of fields in table 3
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_time_ref (
 
			time_id int NOT NULL, 
			start_day float NOT NULL, 
			end_day float NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name time_distribution
 --Number of fields in table 13
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.time_distribution (
 
			cohort_id bigint NOT NULL, 
			count_value float NOT NULL, 
			min_value float NOT NULL, 
			max_value float NOT NULL, 
			average_value float NOT NULL, 
			standard_deviation float NOT NULL, 
			median_value float NOT NULL, 
			p_10_value float NOT NULL, 
			p_25_value float NOT NULL, 
			p_75_value float NOT NULL, 
			p_90_value float NOT NULL, 
			time_metric varchar(50) NOT NULL, 
			database_id varchar(20) NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name visit_context
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.visit_context (
 
			cohort_id bigint NOT NULL, 
			visit_concept_id int NOT NULL, 
			visit_context varchar(20) NOT NULL, 
			subjects float NOT NULL, 
			database_id varchar(20) NOT NULL );  
