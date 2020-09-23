IF OBJECT_ID('tempdb..#unique_concept_ids', 'U') IS NOT NULL
	DROP TABLE #unique_concept_ids;

CREATE TABLE #unique_concept_ids (concept_id INT);
