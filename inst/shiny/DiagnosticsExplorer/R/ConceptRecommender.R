getRecommenderStandardSql <- SqlRender::readSql("sql/RecommendationStandard.sql")
getRecommenderSourceSql <- SqlRender::readSql("sql/RecommendationSource.sql")

getRecommendedConcepts <- function(dataSource, conceptSetSql, standard = TRUE) {
  if (is(dataSource, "environment")) {
    stop("Concept recommender does not work against in-memory data")
  } else {
    conceptSetSql <- SqlRender::render(sql = conceptSetSql, vocabulary_database_schema = dataSource$vocabularyDatabaseSchema)
    if (standard) {
      sql <- getRecommenderStandardSql
    } else {
      sql <- getRecommenderSourceSql
    }
    sql <- SqlRender::render(sql = sql, 
                             target_database_schema = dataSource$resultsDatabaseSchema, 
                             vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                             concept_set_query = conceptSetSql)
    data <- DatabaseConnector::dbGetQuery(dataSource$connection, sql) %>% 
      dplyr::tibble()
    colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
    return(data)
  }
}