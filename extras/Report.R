defaultServer <- Sys.getenv("phenotypeLibraryServer")
defaultDatabase <- Sys.getenv("phenotypeLibrarydb")
defaultPort <- 5432
defaultUser <- Sys.getenv("phenotypeLibrarydbUser")
defaultPassword <- Sys.getenv("phenotypeLibrarydbPw")
defaultResultsSchema <- Sys.getenv("phenotypeLibrarydbTargetSchema")

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = 'postgresql',
  user = defaultUser,
  password = defaultPassword,
  port = defaultPort,
  server = paste0(defaultServer, "/", defaultDatabase)
)
connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)

dataSource <-
  CohortDiagnostics::createDatabaseDataSource(connection = connection,
                                              connectionDetails = connectionDetails, 
                                              resultsDatabaseSchema = defaultResultsSchema, 
                                              vocabularyDatabaseSchema = defaultResultsSchema)

## setUp
databaseId <- 'optum_ehr_v1705_20210811'
databases <- CohortDiagnostics::getResultsDatabase(dataSource = dataSource) %>% 
  dplyr::filter(.data$databaseId == 'optum_ehr_v1705_20210811')
cohorts <- CohortDiagnostics::getResultsCohort(dataSource = dataSource) %>% 
  dplyr::filter(stringr::str_detect(string = tolower(.data$cohortName),
                                    pattern = 'covid')) %>% 
  dplyr::arrange(.data$cohortId)
cohortCount <- CohortDiagnostics::getResultsCohortCount(dataSource = dataSource, 
                                                        databaseIds = c(databases$databaseId) %>% unique, 
                                                        cohortIds = cohorts$cohortId %>% unique())


# initial cohort - id:54/2
# cohort count
cohortCount %>% 
  dplyr::filter(.data$cohortId %in% 2)
characterization <- CohortDiagnostics:::getFeatureExtractionCharacterization(dataSource = dataSource,
                                                                             cohortIds = 2, 
                                                                             databaseIds = databaseId)
characterization$covariateValue %>% 
  dplyr::inner_join(characterization$covariateRef) %>% 
  dplyr::inner_join(characterization$analysisRef) %>% 
  dplyr::filter(.data$domainId == 'Demographics')%>% 
  dplyr::filter(.data$analysisName == 'DemographicsGender')

characterization$covariateValue %>% 
  dplyr::inner_join(characterization$covariateRef) %>% 
  dplyr::inner_join(characterization$analysisRef) %>% 
  # dplyr::filter(.data$domainId == 'Demographics')%>% 
  dplyr::distinct(.data$domainId)
  dplyr::filter(.data$analysisName == 'DemographicsAge')

cohort <-
  CohortDiagnostics::getResultsCohort(dataSource = dataSource, 
                                      cohortIds = c(cohortId))

cohortCount <- 
  CohortDiagnostics::getResultsCohortCount(dataSource = dataSource,
                                           cohortIds = c(cohortId))

conceptSets <- CohortDiagnostics::getResultsConceptSet(dataSource = dataSource,
                                                       cohortIds = c(cohortId))

database <-
  CohortDiagnostics::getResultsDatabase(dataSource = dataSource, 
                                        databaseIds = databaseId)

resolvedConcepts <-
  CohortDiagnostics::getResultsResolvedConcepts(
    dataSource = dataSource,
    databaseIds = c(databaseId),
    cohortIds = c(cohortId)
  )

indexEventBreakdown <-
  CohortDiagnostics::getResultsIndexEventBreakdown(
    dataSource = dataSource,
    daysRelativeIndex = c(0),
    databaseIds = c(databaseId),
    cohortIds = c(cohortId)
  )

concepts <- CohortDiagnostics::getConcept(
  dataSource = dataSource,
  conceptIds = c(indexEventBreakdown$conceptId,
                 resolvedConcepts$conceptId) %>% unique()
)

DatabaseConnector::disconnect(connection)
```

# co-occurrence matrix on index date

```{r report, echo=TRUE}
report <- indexEventBreakdown %>% 
  dplyr::filter(.data$daysRelativeIndex == 0) %>% 
  dplyr::filter(.data$databaseId %in% c(databaseId)) %>% 
  dplyr::filter(.data$cohortId %in% cohortId) %>% 
  dplyr::inner_join(cohortCount, by = c("databaseId", "cohortId")) %>% 
  dplyr::select(-.data$databaseId, -.data$cohortId, -.data$daysRelativeIndex) %>% 
  dplyr::inner_join(concepts %>% 
                      dplyr::filter(.data$domainId %in% c('Visit')) %>% 
                      dplyr::filter(is.na(.data$invalidReason)) %>% 
                      dplyr::select(.data$conceptId, 
                                    .data$conceptName,
                                    .data$vocabularyId,
                                    .data$standardConcept),
                    by = "conceptId")
```

```{r show, echo=TRUE}
report
```
