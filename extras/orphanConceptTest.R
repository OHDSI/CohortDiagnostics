conceptXWalks <-
  DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from #concept_sets_x_walk;",
                                             snakeCaseToCamelCase = TRUE) %>%
  dplyr::tibble() %>%
  dplyr::filter(.data$cohortId == 17493)

startingConcept <-
  DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from OHDSI.dqw4c20vstarting_concepts;",
                                             snakeCaseToCamelCase = TRUE) %>%
  dplyr::tibble() %>%
  dplyr::filter(.data$codesetId == conceptXWalks$uniqueConceptSetId)

synonym <-
  DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from OHDSI.dqw4c20vconcept_synonyms;",
                                             snakeCaseToCamelCase = TRUE) %>%
  dplyr::tibble() %>%
  dplyr::filter(.data$codesetId == conceptXWalks$uniqueConceptSetId)

searchStrings <-
  DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from OHDSI.dqw4c20vsearch_strings;",
                                             snakeCaseToCamelCase = TRUE) %>%
  dplyr::tibble() %>%
  dplyr::filter(.data$codesetId == conceptXWalks$uniqueConceptSetId) %>% 
  dplyr::arrange(codesetId, conceptNameTerms, conceptNameLength)

strTop1000 <-
  DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from OHDSI.dqw4c20vsearch_str_top1000;",
                                             snakeCaseToCamelCase = TRUE) %>%
  dplyr::tibble() %>%
  dplyr::filter(.data$codesetId == conceptXWalks$uniqueConceptSetId) %>% 
  dplyr::arrange(codesetId, conceptNameTerms, conceptNameLength)

searchStringSubset <-
  DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from OHDSI.dqw4c20vsearch_string_subset;",
                                             snakeCaseToCamelCase = TRUE) %>%
  dplyr::tibble() %>%
  dplyr::filter(.data$codesetId == conceptXWalks$uniqueConceptSetId) %>% 
  dplyr::arrange(codesetId, conceptNameTerms, conceptNameLength)

orphanCandidates <-
  DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from OHDSI.dqw4c20vorphan_candidates;",
                                             snakeCaseToCamelCase = TRUE) %>%
  dplyr::tibble() %>%
  dplyr::filter(.data$codesetId == conceptXWalks$uniqueConceptSetId)
