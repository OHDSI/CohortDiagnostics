getResultsDataModelSpecifications() %>% 
  dplyr::distinct(tableName) %>% 
  dplyr::pull() %>% 
  sort()

runCohortCharacterization(cdm,
                          cohortDefinitionSet,
                          temporalCovariateSettings,
                          exportFolder,
                          minCellCount = 5,
                          incremental = FALSE)
# temporal_covariate_ref.csv
# temporal_covariate_value
# temporal_covariate_value_dist
# temporal_analysis_ref
# temporal_time_ref

runCohortRelationship(cdm,
                      cohortDefinitionSet,
                      exportFolder,
                      minCellCount = 5,
                      incremental = FALSE)
# cohort_relationship

runIncidenceRate(cdm,
                 cohortDefinitionSet,
                 exportFolder,
                 minCellCount = 5,
                 incremental = FALSE) 
# incidence_rate.csv


runIncludedSourceConcepts(cdm,
                          cohortDefinitionSet,
                          exportFolder,
                          minCellCount,
                          incremental = FALSE)
# included_source_concepts (plural or not?)

runInclusionStatistics(cdm,
                       cohortDefinitionSet,
                       exportFolder,
                       minCellCount =  5,
                       incremental = FALSE)  
# cohort_inc_stats.csv

runIndexEventBreakdown(cdm,
                       cohortDefinitionSet,
                       exportFolder,
                       minCellCount = 5,
                       incremental = FALSE)
# index_event_breakdown.csv

runOrphanConcepts(cdm,
                  cohortDefinitionSet, # or cohortTableName
                  conceptCountsTable,
                  conceptCountsDatabaseSchema,
                  exportFolder,
                  minCellCount = 5,
                  incremental = FALSE)
# orphan_concepts.csv

runResolvedConcepts(cdm,
                    cohortDefinitionSet,
                    exportFolder,
                    minCellCount = 5,
                    incremental = FALSE)
# resolved_concepts.csv


runVisitContext(cdm,
                cohortDefinitionSet,
                exportFolder,
                minCellCount = 5,
                incremental = FALSE)
# visit_context.csv

runTimeSeries(cdm,
              cohortDefinitionSet,
              exportFolder,
              minCellCount = 5,
              incremental = FALSE)
# time_series.csv

# DrugExposureDiagnostics::executeChecks()
runDrugExposureDiagnostics(cdm,
                           ingredients = c(1125315),
                           subsetToConceptId = NULL,
                           checks = c("missing", "exposureDuration", "quantity"),
                           sample = 10000,
                           tablePrefix = NULL,
                           earliestStartDate = "2010-01-01",
                           verbose = FALSE,
                           byConcept = TRUE,
                           exportFolder,
                           minCellCount = 5,
                           incremental = FALSE)







