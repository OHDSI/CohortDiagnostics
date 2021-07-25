#' @param ExportDetailedVocabulary    (optional) Default TRUE. Do you want to export from source site details of
#'                                    from all vocabulary tables including concept_ancestor, concept_relationship,
#'                                    concept_synonym etc. This is useful if you want to review the impact
#'                                    of the vocabulary used in the data source on cohort definitions, and
#'                                    compare it to the vocabulary used by other data sources or reference. 
#'                                    Please set to FALSE to reduce file size and speed up process - as exporting
#'                                    vocabulary and generating premerged file/deduplication takes significant processing
#'                                    time. If set to FALSE then only 'concept' table  is exported. If set to TRUE (Default)
#'                                    then "concept", "concept_ancestor", "concept_class", "concept_relationship", "concept_synonym", "domain", "relationship"
#'                                    and "vocabulary" are exported.
