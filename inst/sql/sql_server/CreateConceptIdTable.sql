IF OBJECT_ID('tempdb..@table_name', 'U') IS NOT NULL
	DROP TABLE @table_name;

CREATE TABLE @table_name (concept_id INT);
