#' @param cohortDatabaseSchema       Schema name where your user has write access (has CRUD privileges). This is the location,
#'                                   of the cohort tables. This is also the location for the optional table such as 
#'                                   the unique_concept_id table. 
#'                                   In incremental mode: It is assumed that the tables in this location have not been modified 
#'                                   outside this application, and the content was written by this application for current 
#'                                   project only. A good practice would be to ensure that the database table names are unique 
#'                                   for the project. e.g. 'EPI101_COHORT', 'EPI101_UNIQUE_CONCEPT_ID' (please try to keep 
#'                                   length < 22). Another assumption: the content of these tables were written by this application
#'                                   (i.e. they were not altered outside of this application) and the tables are available during the
#'                                   life cycle of this project. The local files that are created by the application during incremental
#'                                   mode such as the 'recordKeepingFile'.
