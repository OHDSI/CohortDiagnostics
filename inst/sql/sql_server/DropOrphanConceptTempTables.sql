IF OBJECT_ID('tempdb..#starting_concepts', 'U') IS NOT NULL
  DROP TABLE #starting_concepts;
  
IF OBJECT_ID('tempdb..#concept_synonyms', 'U') IS NOT NULL
  DROP TABLE #concept_synonyms;
  
IF OBJECT_ID('tempdb..#search_strings', 'U') IS NOT NULL
  DROP TABLE #search_strings;
  
IF OBJECT_ID('tempdb..#search_str_top1000', 'U') IS NOT NULL
  DROP TABLE #search_str_top1000;
  
IF OBJECT_ID('tempdb..#search_string_subset', 'U') IS NOT NULL
  DROP TABLE #search_string_subset;
