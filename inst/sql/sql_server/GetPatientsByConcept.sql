{DEFAULT @domain_table = "domain_table"}
{DEFAULT @domain_concept_id = "domain_concept_id"}
{DEFAULT @scratch = "scratch.dbo"}
{DEFAULT @cdm_schema = "cdm_schema.dbo"}
{DEFAULT @concept_id = "concept_id"}
{DEFAULT @cohort_id = "cohort_id"}

SELECT DISTINCT person_id 
FROM @cdm_schema.@domain_table 
WHERE person_id IN (
    SELECT subject_id 
    FROM @scratch 
    WHERE cohort_definition_id = @cohort_id
) 
AND @domain_concept_id = @concept_id;