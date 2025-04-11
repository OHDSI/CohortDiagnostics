#' @title Query to create a table with most frequent concepts per cohort/domain.
#'
#' @description Query to create a table with the most frequent concepts 
#' from a specific cohort in a specific domain. Archive in inst/sql/sql_server.
#'  
#'  The query uses the following parameters:
#' - @domain_table name of the domain table from the OMOP CDM (e.g., "condition_occurrence").
#' - @domain_concept_id name of the field matching to concept_id from the OMOP CDM (e.g., "condition_concept_id").
#' - @cohort_database_schema scratch space where the cohort is available (e.g., "scratch.dbo").
#' - @cdm_database_schema CDM schema name from the dataset (e.g., "CDM_jmdc_v1063.dbo").
#' - @cohort_id Atlas cohort id of the phenotype to be evaluated (e.g., 5665).
#' - @min_freq Minimum proportion of patients from the cohort having a code to
#' include the code into the output table (default is 5).
#' 
#' @format Plant text SQl file
#'
#' @author Luisa Martínez (10SEP2024)
#' Johnson and Johnson R&D Data Science and Digital Health, Barcelona, Spain  
"GetFrequentConcepts"