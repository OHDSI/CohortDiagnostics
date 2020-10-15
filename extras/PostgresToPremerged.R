# A simple script to extract data from postgres and have a local copy for testing
library(DatabaseConnector)
library(magrittr)

phenotypeIdsToExtract <- c()
cohortIdsToExtract <- c()

connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(Sys.getenv("phenotypeLibraryDbServer"),
                                                            Sys.getenv("phenotypeLibraryDbDatabase"),
                                                            sep = "/"),
                                             port = Sys.getenv("phenotypeLibraryDbPort"),
                                             user = Sys.getenv("phenotypeLibraryDbUser"),
                                             password = Sys.getenv("phenotypeLibraryDbPassword"))
connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
resultsSchema <- Sys.getenv("phenotypeLibraryDbResultsSchema")


resultsDataModel <- readr::read_csv(file = system.file('settings', 'resultsDataModelSpecification.csv',
                                                       package = 'CohortDiagnostics'), 
                                    col_types = readr::cols())

targetLocation <- "D:\\phenotypeLibrary\\presentation"
tablesWithPhenotypeId <- resultsDataModel %>% 
  dplyr::filter(.data$fieldName == 'phenotype_id') %>% 
  dplyr::select(.data$tableName) %>% 
  dplyr::distinct()

for (i in (1:nrow(tablesWithPhenotypeId))) {
  table <- tablesWithPhenotypeId[i,]$tableName
  sql <- "select * from @resultsSchema.@table where phenotype_id in (@phenotypeId)"
  data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     resultsSchema = resultsSchema,
                                                     table = table, 
                                                     phenotypeId = phenotypeIdsToExtract) %>% 
    dplyr::tibble()
  colnames(data) <- tolower(colnames(data))
  assign(x = table, value = data)
  readr::write_excel_csv(x = data, 
                         file = file.path(targetLocation, paste0(table, ".csv")))
} 


######################

tablesWithNoPhenotypeIdHasCohortId <- resultsDataModel %>% 
  dplyr::anti_join(tablesWithPhenotypeId) %>% 
  dplyr::filter(.data$fieldName == 'cohort_id') %>% 
  dplyr::select(.data$tableName) %>% 
  dplyr::distinct()

if (length(cohortIdsToExtract) == 0) {
  cohortIdsToExtract <- cohort$cohort_id %>% unique()
}

for (i in (1:nrow(tablesWithNoPhenotypeIdHasCohortId))) {
  table <- tablesWithNoPhenotypeIdHasCohortId[i,]$tableName
  sql <- "select * from @resultsSchema.@table where cohort_id in (@cohortId)"
  data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     resultsSchema = resultsSchema,
                                                     table = table, 
                                                     cohortId = cohortIdsToExtract) %>% 
    dplyr::tibble()
  colnames(data) <- tolower(colnames(data))
  assign(x = table, value = data)
  readr::write_excel_csv(x = data, 
                         file = file.path(targetLocation, paste0(table, ".csv"), na = ""))
}

otherTables <- resultsDataModel %>% 
  dplyr::anti_join(tablesWithPhenotypeId) %>% 
  dplyr::anti_join(tablesWithNoPhenotypeIdHasCohortId) %>% 
  dplyr::select(.data$tableName) %>% 
  dplyr::distinct()


for (i in (1:nrow(otherTables))) {
  table <- otherTables[i,]$tableName
  sql <- "select * from @resultsSchema.@table"
  data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     resultsSchema = resultsSchema,
                                                     table = table) %>% 
    dplyr::tibble()
  colnames(data) <- tolower(colnames(data))
  assign(x = table, value = data)
  readr::write_excel_csv(x = data, 
                         file = file.path(targetLocation, paste0(table, ".csv"), na = ""))
}

covariate_ref <- covariate_ref %>% 
  dplyr::inner_join(covariate_value %>% 
                      dplyr::select(.data$covariate_id) %>% 
                      dplyr::distinct()) %>% 
  readr::write_excel_csv(file = file.path(targetLocation, "covariate_ref.csv"), na = "")


temporal_covariate_ref <- temporal_covariate_ref %>% 
  dplyr::inner_join(temporal_covariate_value %>% 
                      dplyr::select(.data$covariate_id) %>% 
                      dplyr::distinct()) %>% 
  readr::write_excel_csv(file = file.path(targetLocation, "temporal_covariate_ref.csv"), na = "")

readr::read_csv(file.path(targetLocation, 'cohort.csv')) %>% 
  dplyr::filter(.data$cohort_id %in% cohortIdsToExtract) %>% 
  readr::write_excel_csv(file = file.path(targetLocation, "cohort.csv"), na = "")

files <- list.files(targetLocation, pattern = ".*\\.csv$")
oldWd <- setwd(targetLocation)
on.exit(setwd(targetLocation), add = TRUE)
DatabaseConnector::createZipFile(zipFile = paste0(basename(targetLocation), ".zip"), files = files)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = targetLocation)
