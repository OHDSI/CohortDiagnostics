{DEFAULT @domain_table = "domain_table"}
{DEFAULT @domain_concept_id = "domain_concept_id"}
{DEFAULT @input_concepts = "input_concepts"} -- This parameter should be formatted as a comma-separated string of concept IDs.
{DEFAULT @cdm_schema = "cdm_schema.dbo"}

SELECT @domain_concept_id AS concept_id, 
COUNT(DISTINCT h.person_id) AS total_pop, 
100 * (CONVERT(numeric, COUNT(DISTINCT h.person_id)) / 
         (SELECT COUNT(DISTINCT person_id) FROM @cdm_schema.person)) AS total_pop_perc 
FROM @cdm_schema.@domain_table AS h 
WHERE concept_id IN (@input_concepts) 
GROUP BY 
concept_id;