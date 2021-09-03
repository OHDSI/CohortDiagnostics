# resolveMappedConceptSetFromVocabularyDatabaseSchema <-
#   function(dataSource = .GlobalEnv,
#            conceptSets,
#            vocabularyDatabaseSchema = "vocabulary") {
#     if (is(dataSource, "environment")) {
#       stop("Cannot resolve concept sets without a database connection")
#     } else {
#       sqlBase <-
#         paste(
#           "SELECT DISTINCT codeset_id AS concept_set_id, concept.*",
#           "FROM (",
#           paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
#           ") concept_sets",
#           sep = "\n"
#         )
#       sqlResolved <- paste(
#         sqlBase,
#         "INNER JOIN @vocabulary_database_schema.concept",
#         "  ON concept_sets.concept_id = concept.concept_id;",
#         sep = "\n"
#       )
#       
#       sqlBaseMapped <-
#         paste(
#           "SELECT DISTINCT codeset_id AS concept_set_id,
#                            concept_sets.concept_id AS resolved_concept_id,
#                            concept.*",
#           "FROM (",
#           paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
#           ") concept_sets",
#           sep = "\n"
#         )
#       sqlMapped <- paste(
#         sqlBaseMapped,
#         "INNER JOIN @vocabulary_database_schema.concept_relationship",
#         "  ON concept_sets.concept_id = concept_relationship.concept_id_2",
#         "INNER JOIN @vocabulary_database_schema.concept",
#         "  ON concept_relationship.concept_id_1 = concept.concept_id",
#         "WHERE relationship_id = 'Maps to'",
#         "  AND standard_concept IS NULL;",
#         sep = "\n"
#       )
#       
#       resolved <-
#         renderTranslateQuerySql(
#           connection = dataSource$connection,
#           sql = sqlResolved,
#           vocabulary_database_schema = vocabularyDatabaseSchema,
#           snakeCaseToCamelCase = TRUE
#         ) %>%
#         dplyr::select(
#           .data$conceptSetId,
#           .data$conceptId,
#           .data$conceptName,
#           .data$domainId,
#           .data$vocabularyId,
#           .data$conceptClassId,
#           .data$standardConcept,
#           .data$conceptCode,
#           .data$invalidReason
#         ) %>%
#         dplyr::arrange(.data$conceptId)
#       mapped <-
#         renderTranslateQuerySql(
#           connection = dataSource$connection,
#           sql = sqlMapped,
#           vocabulary_database_schema = vocabularyDatabaseSchema,
#           snakeCaseToCamelCase = TRUE
#         ) %>%
#         dplyr::select(
#           .data$resolvedConceptId,
#           .data$conceptId,
#           .data$conceptName,
#           .data$domainId,
#           .data$vocabularyId,
#           .data$conceptClassId,
#           .data$standardConcept,
#           .data$conceptCode,
#           .data$conceptSetId
#         ) %>%
#         dplyr::distinct() %>%
#         dplyr::arrange(.data$resolvedConceptId, .data$conceptId)
#     }
#     data <- list(resolved = resolved, mapped = mapped)
#     return(data)
#   }
# 
# getCohortOverlapResult <- function(dataSource = .GlobalEnv,
#                                    targetCohortIds,
#                                    comparatorCohortIds,
#                                    databaseIds) {
#   table <- 'cohortOverlap'
#   if (is(dataSource, "environment")) {
#     if (!exists(table)) {
#       return(NULL)
#     }
#     if (length(table) == 0) {
#       return(NULL)
#     }
#     if (nrow(get(table, envir = dataSource)) == 0) {
#       return(NULL)
#     }
#     data <- get(table, envir = dataSource) %>%
#       dplyr::filter(
#         .data$targetCohortId %in% !!targetCohortIds &
#           .data$comparatorCohortId %in% !!comparatorCohortIds &
#           .data$databaseId %in% !!databaseIds
#       ) %>%
#       dplyr::inner_join(
#         dplyr::select(
#           get("cohort", envir = dataSource),
#           targetCohortId = .data$cohortId,
#           targetCohortName = .data$cohortName,
#           cohortName = .data$cohortName
#         ),
#         by = "targetCohortId"
#       ) %>%
#       dplyr::inner_join(
#         dplyr::select(
#           get("cohort", envir = dataSource),
#           comparatorCohortId = .data$cohortId,
#           comparatorCohortName = .data$cohortName
#         ),
#         by = "comparatorCohortId"
#       )
#   } else {
#     sql <-   "SELECT cohort_overlap.*,
#                 target_cohort.cohort_name AS target_cohort_name,
#                 comparator_cohort.cohort_name AS comparator_cohort_name
#               FROM  @results_database_schema.cohort_overlap
#               INNER JOIN @results_database_schema.cohort target_cohort
#                 ON cohort_overlap.target_cohort_id = target_cohort.cohort_id
#               INNER JOIN @results_database_schema.cohort comparator_cohort
#                 ON cohort_overlap.comparator_cohort_id = comparator_cohort.cohort_id
#               WHERE target_cohort_id in (@targetCohortId)
#               AND comparator_cohort_id in (@comparatorCohortId)
#             	AND database_id in (@databaseId);"
#     data <-
#       renderTranslateQuerySql(
#         connection = dataSource$connection,
#         sql = sql,
#         results_database_schema = dataSource$resultsDatabaseSchema,
#         targetCohortId = targetCohortIds,
#         comparatorCohortId = comparatorCohortIds,
#         databaseId = quoteLiterals(databaseIds),
#         snakeCaseToCamelCase = TRUE
#       ) %>%
#       tidyr::tibble()
#   }
#   if (nrow(data) == 0) {
#     return(NULL)
#   }
#   return(data)
# }
# 
# 
# pivotOrphanConceptResult <- function(data,
#                                      dataSource) {
#   databaseIds <- unique(data$databaseId)
#   maxCount <- max(data$conceptCount, na.rm = TRUE)
#   table <- data %>%
#     dplyr::select(.data$databaseId,
#                   .data$conceptId,
#                   .data$subjectCount,
#                   .data$conceptCount) %>%
#     dplyr::group_by(.data$databaseId,
#                     .data$conceptId) %>%
#     dplyr::summarise(
#       conceptSubjects = sum(.data$subjectCount),
#       conceptCount = sum(.data$conceptCount),
#       .groups = 'keep'
#     ) %>%
#     dplyr::ungroup() %>%
#     dplyr::arrange(.data$databaseId) %>%
#     tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>%
#     dplyr::mutate(name = paste0(
#       databaseId,
#       "_",
#       stringr::str_replace(
#         string = .data$name,
#         pattern = "concept",
#         replacement = ""
#       )
#     )) %>%
#     tidyr::pivot_wider(
#       id_cols = c(.data$conceptId),
#       names_from = .data$name,
#       values_from = .data$value,
#       values_fill = 0
#     )
#   conceptIdDetails <- getConcept(dataSource = dataSource,
#                                             conceptIds = table$conceptId %>% unique())
#   table <- table %>%
#     dplyr::inner_join(
#       conceptIdDetails %>%
#         dplyr::select(
#           .data$conceptId,
#           .data$conceptName,
#           .data$vocabularyId,
#           .data$conceptCode
#         ) %>%
#         dplyr::distinct(),
#       by = "conceptId"
#     ) %>%
#     dplyr::relocate(.data$conceptId,
#                     .data$conceptName,
#                     .data$vocabularyId,
#                     .data$conceptCode)
#   table <- table[order(-table[, 5]),]
#   attr(x = table, which = "databaseIds") <- databaseIds
#   attr(x = table, which = "maxCount") <- maxCount
#   return(table)
# }
