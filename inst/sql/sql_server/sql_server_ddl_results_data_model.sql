--DDL Specification for package CohortDiagnostics package version: 2.0
 --Data Model Version 2.0
 --Last update 2020-09-13
 --Number of tables 8
 
 ----------------------------------------------------------------------- 
 --Table name analysis_ref
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.analysis_ref (
 
			analysis_id bigint NOT NULL, 
			analysis_name varchar(20) NOT NULL, 
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
 --Table name cohort_inclusion
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_inclusion (
 
			cohort_definition_id bigint NOT NULL, 
			rule_sequence float NOT NULL, 
			name varchar(255) NOT NULL, 
			description varchar(max) NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_inclusion_result
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_inclusion_result (
 
			cohort_definition_id bigint NOT NULL, 
			inclusion_rule_mask float NOT NULL, 
			person_count float NOT NULL, 
			mode_id bigint NOT NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_inclusion_stats
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_inclusion_stats (
 
			cohort_definition_id bigint NOT NULL, 
			rule_sequence float NOT NULL, 
			person_count float NOT NULL, 
			gain_count float NOT NULL, 
			person_total float NOT NULL, 
			mode_id bigint NOT NULL );  
 
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
 --Table name cohort_summary_stats
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_summary_stats (
 
			cohort_definition_id bigint NOT NULL, 
			base_count float NOT NULL, 
			final_count float NOT NULL, 
			mode_id bigint NOT NULL );  
