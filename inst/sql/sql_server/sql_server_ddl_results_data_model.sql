--DDL Specification for package CohortDiagnostics package version: 2.0
 --Data Model Version 2.0
 --Last update 2020-09-14
 --Number of tables 30
 
 ----------------------------------------------------------------------- 
 --Table name analysis_ref
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.analysis_ref (
 
			analysis_id bigint , 
			analysis_name text , 
			domain_id text NULL, 
			start_day float NULL, 
			end_day float NULL, 
			is_binary text , 
			missing_means_zero text NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort (
 
			referent_concept_id bigint , 
			cohort_id bigint , 
			web_api_cohort_id bigint , 
			cohort_name text , 
			logic_description varchar(max) , 
			sql varchar(max) , 
			json varchar(max)  );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_count
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_count (
 
			cohort_id bigint , 
			cohort_entries float , 
			cohort_subjects float , 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_inclusion
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_inclusion (
 
			cohort_id bigint , 
			rule_sequence float , 
			name text , 
			description varchar(max) NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_inclusion_result
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_inclusion_result (
 
			cohort_id bigint , 
			inclusion_rule_mask float , 
			person_count float , 
			mode_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_inclusion_stats
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_inclusion_stats (
 
			cohort_id bigint , 
			rule_sequence float , 
			person_count float , 
			gain_count float , 
			person_total float , 
			mode_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_overlap
 --Number of fields in table 12
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_overlap (
 
			either_subjects float , 
			both_subjects float , 
			t_only_subjects float , 
			c_only_subjects float , 
			t_before_c_subjects float , 
			c_before_t_subjects float , 
			same_day_subjects float , 
			t_in_c_subjects float , 
			c_in_t_subjects float , 
			target_cohort_id bigint , 
			comparator_cohort_id bigint , 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name cohort_summary_stats
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.cohort_summary_stats (
 
			cohort_id bigint , 
			base_count float , 
			final_count float , 
			mode_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name concept
 --Number of fields in table 10
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept (
 
			concept_id bigint , 
			concept_name text NULL, 
			domain_id text , 
			vocabulary_id text , 
			concept_class_id text , 
			standard_concept text NULL, 
			concept_code text NULL, 
			valid_start_date Date , 
			valid_end_date Date , 
			invalid_reason text NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_ancestor
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_ancestor (
 
			ancestor_concept_id bigint , 
			descendant_concept_id bigint , 
			min_levels_of_separation float , 
			max_levels_of_separation float  );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_class
 --Number of fields in table 3
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_class (
 
			concept_class_id text , 
			concept_class_name text , 
			concept_class_concept_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_relationship
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_relationship (
 
			concept_id_1 bigint , 
			concept_id_2 bigint , 
			relationship_id text , 
			valid_start_date Date , 
			valid_end_date Date , 
			invalid_reason text NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_sets
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_sets (
 
			cohort_id bigint , 
			concept_set_id bigint , 
			concept_set_sql varchar(max) , 
			concept_set_name text , 
			concept_set_expression varchar(max)  );  
 
 ----------------------------------------------------------------------- 
 --Table name concept_synonym
 --Number of fields in table 3
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.concept_synonym (
 
			concept_id bigint , 
			concept_synonym_name varchar(max) NULL, 
			language_concept_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name covariate_ref
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.covariate_ref (
 
			covariate_id bigint , 
			covariate_name text , 
			covariate_analysis_id bigint , 
			concept_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name covariate_value
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.covariate_value (
 
			cohort_id bigint , 
			covariate_id bigint , 
			mean float , 
			sd float NULL, 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name database
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.database (
 
			database_id text , 
			database_name text , 
			description varchar(max) , 
			is_meta_analysis float  );  
 
 ----------------------------------------------------------------------- 
 --Table name domain
 --Number of fields in table 3
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.domain (
 
			domain_id text , 
			domain_name text , 
			domain_concept_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name incidence_rate
 --Number of fields in table 8
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.incidence_rate (
 
			cohort_count float , 
			person_years float , 
			gender text NULL, 
			age_group text NULL, 
			calendar_year float NULL, 
			incidence_rate float , 
			cohort_id bigint , 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name included_source_concept
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.included_source_concept (
 
			database_id text , 
			cohort_id bigint , 
			concept_set_id bigint , 
			concept_id bigint , 
			source_concept_id bigint , 
			concept_subjects float , 
			concept_count float  );  
 
 ----------------------------------------------------------------------- 
 --Table name inclusion_rule_stats
 --Number of fields in table 7
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.inclusion_rule_stats (
 
			rule_sequence float , 
			name text , 
			person_count float , 
			gain_count float , 
			person_total float , 
			cohort_id bigint , 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name index_event_breakdown
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.index_event_breakdown (
 
			concept_id bigint , 
			concept_name text , 
			concept_count float , 
			cohort_id bigint , 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name orphan_concept
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.orphan_concept (
 
			database_id text , 
			cohort_id bigint , 
			concept_set_id bigint , 
			concept_id bigint , 
			concept_count float  );  
 
 ----------------------------------------------------------------------- 
 --Table name relationship
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.relationship (
 
			relationship_id text , 
			relationship_name text , 
			is_hierarchical float , 
			defines_ancestry float , 
			reverse_relationship_id text , 
			relationship_concept_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_analysis_ref
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_analysis_ref (
 
			analysis_id bigint , 
			analysis_name text , 
			domain_id text , 
			is_binary text , 
			missing_means_zero text NULL );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_covariate_ref
 --Number of fields in table 4
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_covariate_ref (
 
			covariate_id bigint , 
			covariate_name text , 
			covariate_analysis_id bigint , 
			concept_id bigint  );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_covariate_value
 --Number of fields in table 6
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_covariate_value (
 
			cohort_id bigint , 
			time_id bigint , 
			covariate_id bigint , 
			mean float , 
			sd float NULL, 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name temporal_time_ref
 --Number of fields in table 3
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.temporal_time_ref (
 
			time_id bigint , 
			start_day float , 
			end_day float  );  
 
 ----------------------------------------------------------------------- 
 --Table name time_distribution
 --Number of fields in table 13
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.time_distribution (
 
			cohort_id bigint , 
			count_value float , 
			min_value float , 
			max_value float , 
			average_value float , 
			standard_deviation float , 
			median_value float , 
			p_10_value float , 
			p_25_value float , 
			p_75_value float , 
			p_90_value float , 
			time_metric text , 
			database_id text  );  
 
 ----------------------------------------------------------------------- 
 --Table name vocabulary
 --Number of fields in table 5
 --HINT DISTRIBUTE ON RANDOM
 CREATE TABLE @resultsDatabaseSchema.vocabulary (
 
			vocabulary_id text , 
			vocabulary_name text , 
			vocabulary_reference text NULL, 
			vocabulary_version text NULL, 
			vocabulary_concept_id bigint  );  
