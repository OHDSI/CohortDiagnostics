## Step 1: Specify output location
locationForResults <- file.path("D:\\results\\conceptSets")
dir.create(path = locationForResults, 
           showWarnings = FALSE,
           recursive = TRUE)

# Step 2: Please provide an array of relevant search string
keyWords <- c("Blurred Vision")

#######################

outputLocation <- stringr::str_replace_all(string = keyWords[[1]], 
                                           pattern = " ",
                                           replacement = "")

vocabularyIdOfInterest <- c('SNOMED', 'HCPCS', 'ICD10CM', 'ICD10', 'ICD9CM', 'ICD9', 'Read')
domainIdOfInterest <- c('Condition', 'Observation')
# Details for connecting to the server:
connectionDetails <-
  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = paste(
      Sys.getenv("shinydbServer"),
      Sys.getenv("shinydbDatabase"),
      sep = "/"
    ),
    user = Sys.getenv("shinydbUser"),
    password = Sys.getenv("shinydbPw"),
    port = Sys.getenv("shinydbPort")
  )

connection <-
  DatabaseConnector::connect(connectionDetails = connectionDetails)


# get search results
searchResult <- list()

### iteration 1
for (i in (1:length(keyWords))) {
  locationForResults2 <- file.path(locationForResults,
                                   paste0('keyWord', keyWords[[i]]),
                                   'iteration1')
  # step perform string search
  searchResultConceptIds <-
    CohortDiagnostics:::getStringSearchConcepts(
      connection = connection,
      connectionDetails = connectionDetails,
      vocabularyDatabaseSchema = "vocabulary",
      searchString = keyWords[[i]]
    )
  if (length(vocabularyIdOfInterest) > 0) {
    searchResultConceptIds <- searchResultConceptIds %>%
      dplyr::filter(.data$vocabularyId %in% vocabularyIdOfInterest)
  }
  if (length(domainIdOfInterest) > 0) {
    searchResultConceptIds <- searchResultConceptIds %>%
      dplyr::filter(.data$domainId %in% domainIdOfInterest)
  }
  
  conceptSetExpression <- list()
  conceptSetExpression$items <- list()
  
  for (i in (1:nrow(searchResultConceptIds))) {
    conceptSetExpression$items[i]$concept <- searchResultConceptIds[i,] %>% 
      dplyr::select(.data$conceptClassId,
                    .data$conceptCode,
                    .data$conceptId, 
                    .data$conceptName,
                    .data$domainId,
                    .data$invalidReason,
                    .data$standardConcept,
                    .data$vocabularyId
                    )
    
    conceptSetExpression$items[[i]]$isExcluded = FALSE
    conceptSetExpression$items[[i]]$includeDescendants = TRUE
    conceptSetExpression$items[[i]]$includeMapped = TRUE
    
    conceptSetExpression$items[[i]] <- RJSONIO::toJSON(x = conceptSetExpression$items[[i]],
                                                       digits = 23)
  }
  
  conceptSetExpression$concept <- searchResultConceptIds %>% 
    dplyr::select(.data$conceptId) %>% 
    dplyr::distinct() %>% 
    dplyr::rename(CONCEPT_ID = .data$conceptId) %>% 
    RJSONIO::toJSON(digits = 23, pretty = TRUE) %>% 
    writeLines()
  
  # develop a concept set expression based on string search
  conceptSetExpressionDataFrame <-
    CohortDiagnostics:::getOptimizationRecommendationForConceptSetExpression(
      conceptSetExpression = conceptSetExpression,
      vocabularyDatabaseSchema = "vocabulary",
      connection = connection
    )
    getConceptSetExpressionFromConceptSetExpressionDataFrame(conceptSetExpressionDataFrame = searchResultConceptIds,
                                                             selectAllDescendants = TRUE) %>%
    getConceptSetSignatureExpression(connection = connection) %>%
    getConceptSetExpressionDataFrameFromConceptSetExpression(
      updateVocabularyFields = TRUE,
      connection = connection,
      connectionDetails = connectionDetails
    )
  
  conceptSetExpression <-
    getConceptSetExpressionFromConceptSetExpressionDataFrame(conceptSetExpressionDataFrame = conceptSetExpressionDataFrame)
  
  
  # resolve concept set expression to individual concept ids
  resolvedConceptIds <-
    resolveConceptSetExpression(connection = connection,
                                conceptSetExpression = conceptSetExpression)
  
  recommendedConceptIds <-
    getRecommendationForConceptSetExpression(
      conceptSetExpression = conceptSetExpression,
      connection = connection,
      connectionDetails = connectionDetails,
      vocabularyIdOfInterest = vocabularyIdOfInterest,
      domainIdOfInterest = domainIdOfInterest
    )
  
  searchResult <- list(
    searchString = searchString,
    searchResultConceptIds = searchResultConceptIds,
    conceptSetExpressionDataFrame = conceptSetExpressionDataFrame,
    resolvedConceptIds = resolvedConceptIds,
    recommendedConceptIds = recommendedConceptIds
  )
  
  if (exportResults) {
    if (!is.null(locationForResults)) {
      dir.create(path = locationForResults,
                 showWarnings = FALSE,
                 recursive = TRUE)
      if (nrow(recommendedConceptIds$recommendedStandard) > 0) {
        readr::write_excel_csv(
          x = recommendedConceptIds$recommendedStandard,
          file = file.path(
            locationForResults,
            paste0("recommendedStandard.csv")
          ),
          append = FALSE,
          na = ""
        )
        writeLines(text = paste0(
          "Wrote recommendedStandard.csv to ",
          locationForResults
        ))
      } else {
        writeLines(
          text = paste0(
            "No recommendation. recommendedStandard.csv is not written to ",
            locationForResults
          )
        )
        unlink(
          x = file.path(
            locationForResults,
            paste0("recommendedStandard.csv")
          ),
          recursive = TRUE,
          force = TRUE
        )
      }
      if (nrow(recommendedConceptIds$recommendedSource) > 0) {
        readr::write_excel_csv(
          x = recommendedConceptIds$recommendedSource,
          file = file.path(locationForResults,
                           paste0("recommendedSource.csv")),
          append = FALSE,
          na = ""
        )
        writeLines(text = paste0("Wrote recommendedSource.csv to ",
                                 locationForResults))
      } else {
        writeLines(
          text = paste0(
            "No recommendation. recommendedSource.csv is not written to ",
            locationForResults
          )
        )
        unlink(
          x = file.path(locationForResults,
                        paste0("recommendedSource.csv")),
          recursive = TRUE,
          force = TRUE
        )
      }
    }
  }
  searchResult[[i]] <- designDiagnostics
}

saveRDS(object = searchResult, file = file.path(locationForResults, "searchResult.rds"))
if (length(searchResult) >  1) {
  conceptSetExpressionAllTerms <- list()
  for (i in (1:length(searchResult))) {
    conceptSetExpressionAllTerms[[i]] <- searchResult[[i]]$conceptSetExpressionDataFrame
  }
  conceptSetExpressionAllTerms <- dplyr::bind_rows(conceptSetExpressionAllTerms) %>% 
    ConceptSetDiagnostics::getConceptSetExpressionFromConceptSetExpressionDataFrame() %>% 
    ConceptSetDiagnostics::getConceptSetSignatureExpression(connection = connection) %>% 
    ConceptSetDiagnostics::getConceptSetExpressionDataFrameFromConceptSetExpression(connection = connection, 
                                                                                    updateVocabularyFields = TRUE) %>% 
    ConceptSetDiagnostics::getConceptSetExpressionFromConceptSetExpressionDataFrame()
  
  json <- conceptSetExpressionAllTerms %>% 
    RJSONIO::toJSON(digits = 23, pretty = TRUE)
  
  SqlRender::writeSql(sql = json,
                      targetFile = file.path(locationForResults, "conceptSetExpressionAllTerms.json"))
}


for (j in (1:length(searchResult))) {
  json <- ConceptSetDiagnostics::getConceptSetExpressionFromConceptSetExpressionDataFrame(
    conceptSetExpressionDataFrame = designDiagnostics$conceptSetExpressionDataFrame) %>% 
    RJSONIO::toJSON(digits = 23, pretty = TRUE) 
  SqlRender::writeSql(sql = json,
                      targetFile = file.path(locationForResults, paste0("conceptSetExpression", j, ".json")))
}

DatabaseConnector::disconnect(connection = connection)

