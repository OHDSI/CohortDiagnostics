library(magrittr)

source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")

# shinySettings <- list(connectionDetails = DatabaseConnector::createConnectionDetails(dbms = "postgresql",
#                                              server = "localhost/ohdsi",
#                                              user = "postgres",
#                                              password = Sys.getenv("pwPostgres")),
#                       resultsDatabaseSchema =  "phenotype_library",
#                       vocabularyDatabaseSchema =  "phenotype_library")
# shinySettings <- list(dataFolder = "s:/examplePackageOutput")

# Settings when running on server:
defaultLocalDataFolder <- "data"
defaultLocalDataFile <- "PreMerged.RData"

connectionPool <- NULL
defaultServer <- Sys.getenv("phoebedbServer")
defaultDatabase <- Sys.getenv("phoebedb")
defaultPort <- 5432
defaultUser <- Sys.getenv("phoebedbUser")
defaultPassword <- Sys.getenv("phoebedbPw")
defaultResultsSchema <- Sys.getenv("phoebedbTargetSchema")
defaultVocabularySchema <- Sys.getenv("phoebedbVocabSchema")

defaultDatabaseMode <- FALSE # Use file system if FALSE

defaultCohortBaseUrl <- "https://atlas.ohdsi.org/#/cohortdefinition/"
defaultConceptBaseUrl <- "https://athena.ohdsi.org/search-terms/terms/"

cohortDiagnosticModeDefaultTitle <- "Cohort Diagnostics"
phenotypeLibraryModeDefaultTitle <- "Phenotype Library"

defaultAboutText <- "<table>
  <tr>
    <td>
      <h3>Phenotype Library</h3>
    </td>
    <td style=\"text-align: right;\">
      <img src=\"https://avatars2.githubusercontent.com/u/6570077?s=280&v=4\", width=100, height=100>
    </td>
  </tr>
  <tr>
    <td>
      <p>OHDSI Phenotype Library is an open community resource maintained by the OHDSI community to support phenotype development, evaluation, sharing and re-use. The Phenotype Library is maintained by community librarians. They are volunteer collaborators who are curating the content contributed by the rest of the community to ensure it is appropriately organized and conforms to community library standards.</p>
      <p>The OHDSI Phenotype work group is responsible to facilitate the generation and maintenance of  the content in the library. To be included, every cohort definition is expected to belong to one Phenotype, and it should have at least one full result set from Cohort Diagnostics executed on  at least one data source. The output should have been contributed to the Phenotype library.</p>
      <p>All cohort definitions in the phenotype library are expressed in JSON and SQL (OHDSI SQL) instructions that are compatible with OHDSI analytic tools and OHDSI OMOP CDM v5.0+. Currently, cohort definitions in the Phenotype Library are implemented in OHDSI SQL compatible with OMOP CDM v5.0+, with JSON specifications compatible with the OHDSI ATLAS platform. Supporting content is organized in the respective folders: literature, notes and evaluation. Literature review is organized using a standardized template</p>
      <p><strong>How to download the library contents: </strong>To download the full set of phenotypes and cohort definitions please go to the <a href=\"https://github.com/ohdsi/phenotypeLibrary\", target=\"_blank\">OHDSI Phenotype Library GitHub repository</a>.</p>
      <p><strong>How to contribute a full set of phenotype library diagnostics across the full library:</strong> Please execute the <a href =\"https://github.com/ohdsi-studies/phenotypeLibraryDiagnostics/\", target=\"_blank\">Phenotype Library Diagnostics</a> study package and submit the results to the coordinating site.</p>
      <p><strong>How to run diagnostics on your cohorts using Cohort Diagnostics:</strong> You can develop your own cohort using ATLAS and evaluate it using CohortDiagnostics, by following the instructions at <a href=\"https://github.com/ohdsi/cohortdiagnostics\", target=\"_blank\">OHDSI Cohort Diagnostics.</a>.</p>
	 </td>
	</tr>
    <tr>
      <td>
        <h4><strong>How to Contribute:</strong></h4>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJURVA3NThUWU42RjRUWDVZWlpUNjM2OVlIWSQlQCN0PWcu\", target=\"_blank\"><h5>Add a phenotype</h5></a>
		<p>Add a phenotype' should be completed if you are interested in submitting a new phenotype to the library. The minimum required elements to contribute a new phenotype are:  1) Phenotype Name, 2) Clinical Description, 3) At least one cohort definition, with logical description and JSON specification, 4) at least one Cohort Diagnostics resultset from a database.</p>
	  </td>
      <td>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJURVA3NThUWU42RjRUWDVZWlpUNjM2OVlIWSQlQCN0PWcu\", target=\"_blank\"><img src=\"Add-Phenotype.png\", width=300></a>
	  </td>
    </tr>
	<tr>
      <td>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJUNllaNVk5NUIwOTRIUzVMTVRDSkdHWFVHRCQlQCN0PWcu\", target=\"_blank\"><h5>Add a cohort definition</h5></a>
        <p>'Add a cohort definition' should be completed if you are interested in submitting a new cohort definition to an existing phenotype within the OHDSI Phenotype Library.  The minimum required elements to contribute a new cohort definition are:  1) Cohort Definition Name, 2) Existing Phenotype Name, 3) Logical Description, 4) JSON Specification, 5) at least one Cohort Diagnostics resultset from a database.</p>
	  </td>	
      <td>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJUNllaNVk5NUIwOTRIUzVMTVRDSkdHWFVHRCQlQCN0PWcu\", target=\"_blank\"><img src=\"Add-Cohort-Definition.png\", width=300>
	  </td>
	 </td>
	 <tr>
      <td>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJURFJORE1LUERHV1lNNlRGNEU5TDgwTlZXVCQlQCN0PWcu\", target=\"_blank\"><h5>Add Diagnostics</h5></a>
        <p>'Add diagnostics' should be completed if you are interested in submitting new diagnostics results to an existing Phenotype/Cohort Definition in the OHDSI Phenotype Library.</p>
      </td>
      <td>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJURFJORE1LUERHV1lNNlRGNEU5TDgwTlZXVCQlQCN0PWcu\", target=\"_blank\"><img src=\"Add-Diagnostics.png\", width=300></a>
	  </td>
	</tr>
    <tr>
      <td>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJUMTdMUkhUOFE1SFpPRVlKSzlEMkxCN1JRUiQlQCN0PWcu\", target=\"_blank\"><h5>Add Insights</h5></a>
        <p>'Add insights' should be completed if you are interested in submitting new insight or information to an existing Phenotype/Cohort Definition in the OHDSI Phenotype Library.</p>
	  </td>
      <td>
        <a href=\"https://forms.office.com/Pages/ResponsePage.aspx?id=lAAPoyCRq0q6TOVQkCOy1aDcZLTRBnxHtm0Rgn5NBBJUMTdMUkhUOFE1SFpPRVlKSzlEMkxCN1JRUiQlQCN0PWcu\", target=\"_blank\"><img src=\"Add-Insights.png\", width=300></a>
	  </td>
	</tr>
</table>"

if (!exists("shinySettings")) {
  writeLines("Using default settings")
  databaseMode <- defaultDatabaseMode & defaultServer != ""
  if (databaseMode) {
    connectionPool <- pool::dbPool(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = "postgresql",
      server = paste(defaultServer, defaultDatabase, sep = "/"),
      port = defaultPort,
      user = defaultUser,
      password = defaultPassword
    )
    resultsDatabaseSchema <- defaultResultsSchema
    vocabularyDatabaseSchema <- defaultVocabularySchema
  } else {
    dataFolder <- defaultLocalDataFolder
  }
  cohortBaseUrl <- defaultCohortBaseUrl
  conceptBaseUrl <- defaultConceptBaseUrl
  if (!is.null(defaultAboutText)) {
    aboutText <- defaultAboutText
  } 
} else {
  writeLines("Using settings provided by user")
  databaseMode <- !is.null(shinySettings$connectionDetails)
  if (databaseMode) {
    connectionDetails <- shinySettings$connectionDetails
    if (is(connectionDetails$server, "function")) {
      connectionPool <- pool::dbPool(drv = DatabaseConnector::DatabaseConnectorDriver(),
                                     dbms = "postgresql",
                                     server = connectionDetails$server(),
                                     port = connectionDetails$port(),
                                     user = connectionDetails$user(),
                                     password = connectionDetails$password(),
                                     connectionString = connectionDetails$connectionString())
    } else {
      # For backwards compatibility with older versions of DatabaseConnector:
      connectionPool <- pool::dbPool(drv = DatabaseConnector::DatabaseConnectorDriver(),
                                     dbms = "postgresql",
                                     server = connectionDetails$server,
                                     port = connectionDetails$port,
                                     user = connectionDetails$user,
                                     password = connectionDetails$password,
                                     connectionString = connectionDetails$connectionString)
    }
    resultsDatabaseSchema <- shinySettings$resultsDatabaseSchema
    vocabularyDatabaseSchema <- shinySettings$vocabularyDatabaseSchema
  } else {
    dataFolder <- shinySettings$dataFolder
  }
  cohortBaseUrl <- shinySettings$cohortBaseUrl
  conceptBaseUrl <- shinySettings$cohortBaseUrl
  if (!is.null(shinySettings$aboutText)) {
    aboutText <- shinySettings$aboutText
  }
}

dataModelSpecifications <- read.csv("resultsDataModelSpecification.csv")
# Cleaning up any tables in memory:
suppressWarnings(rm(list = SqlRender::snakeCaseToCamelCase(dataModelSpecifications$tableName)))

if (databaseMode) {
  
  onStop(function() {
    if (DBI::dbIsValid(connectionPool)) {
      writeLines("Closing database pool")
      pool::poolClose(connectionPool)
    }
  })
  
  resultsTablesOnServer <- tolower(DatabaseConnector::dbListTables(connectionPool, schema = resultsDatabaseSchema))
  
  loadResultsTable <- function(tableName, required = FALSE) {
    if (required || tableName %in% resultsTablesOnServer) {
      tryCatch({
        table <- DatabaseConnector::dbReadTable(connectionPool, 
                                                paste(resultsDatabaseSchema, tableName, sep = "."))
      }, error = function(err) {
        stop("Error reading from ", paste(resultsDatabaseSchema, tableName, sep = "."), ": ", err$message)
      })
      colnames(table) <- SqlRender::snakeCaseToCamelCase(colnames(table))
      if (nrow(table) > 0) {
        assign(SqlRender::snakeCaseToCamelCase(tableName), dplyr::as_tibble(table), envir = .GlobalEnv)
      }
    }
  }
  
  loadResultsTable("database", required = TRUE)
  loadResultsTable("cohort", required = TRUE)
  loadResultsTable("cohort_extra")
  loadResultsTable("phenotype_description")
  loadResultsTable("temporal_time_ref")
  loadResultsTable("concept_sets")
  
  # Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
  isEmpty <- function(tableName) {
    sql <- sprintf("SELECT 1 FROM %s.%s LIMIT 1;", resultsDatabaseSchema, tableName)
    oneRow <- DatabaseConnector::dbGetQuery(connectionPool, sql)
    return(nrow(oneRow) == 0)
  }
  
  for (table in c(dataModelSpecifications$tableName, "recommender_set")) {
    if (table %in% resultsTablesOnServer && 
        !exists(SqlRender::snakeCaseToCamelCase(table)) &&
        !isEmpty(table)) {
      assign(SqlRender::snakeCaseToCamelCase(table), dplyr::tibble())
    }
  }
  
  dataSource <- createDatabaseDataSource(connection = connectionPool,
                                         resultsDatabaseSchema = resultsDatabaseSchema,
                                         vocabularyDatabaseSchema = vocabularyDatabaseSchema)
} else {
  localDataPath <- file.path(dataFolder, defaultLocalDataFile)
  if (!file.exists(localDataPath)) {
    stop(sprintf("Local data file %s does not exist.", localDataPath))
  }
  dataSource <- createFileDataSource(localDataPath, envir = .GlobalEnv)
}

if (exists("cohort")) {
  cohort <- get("cohort") 
  if ('phenotypeId' %in% colnames(cohort)) {
    pId <- cohort %>% 
      dplyr::select(.data$phenotypeId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange(.data$phenotypeId) %>% 
      dplyr::filter(!is.na(.data$phenotypeId)) %>% 
      dplyr::mutate(shortNamePhenotypeId = paste("P", dplyr::row_number()))
    
    cohort <- cohort %>%
      dplyr::left_join(pId, by = "phenotypeId") %>% 
      dplyr::mutate(shortNamePhenotypeId = tidyr::replace_na(data = .data$shortNamePhenotypeId, replace = "")) %>% 
      dplyr::group_by(.data$phenotypeId) %>% 
      dplyr::arrange(.data$cohortId) %>% 
      dplyr::mutate(shortName = paste0(.data$shortNamePhenotypeId,
                                       " ", 
                                       "C", 
                                       dplyr::row_number())) %>%
      dplyr::mutate(compoundName = paste(.data$shortName, .data$cohortName)) %>% 
      dplyr::select(-.data$shortNamePhenotypeId) %>% 
      dplyr::ungroup(.data$phenotypeId)
  } else {
    cohort <- cohort %>%
      dplyr::arrange(.data$cohortId) %>% 
      dplyr::mutate(shortName = paste0("C", dplyr::row_number())) %>% 
      dplyr::mutate(compoundName = paste0(.data$shortName, .data$cohortName))
  }
}


if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- get("temporalTimeRef") %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}

if (exists("covariateRef")) {
  specifications <- readr::read_csv(file = "Table1Specs.csv", 
                                    col_types = readr::cols(),
                                    guess_max = min(1e7))
  prettyAnalysisIds <- specifications$analysisId
}

if (exists("phenotypeDescription")) {
  phenotypeDescription <- phenotypeDescription %>% 
    dplyr::mutate(overview = (stringr::str_match(.data$clinicalDescription, 
                                                 "Overview:(.*?)Presentation:"))[,2] %>%
                    stringr::str_squish() %>% 
                    stringr::str_trim()) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                 pattern = "Overview:", 
                                                                 replacement = "<strong>Overview:</strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                 pattern = "Assessment:", 
                                                                 replacement = "<br/><br/> <strong>Assessment:</strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                 pattern = "Presentation:", 
                                                                 replacement = "<br/><br/> <strong>Presentation: </strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription,
                                                                 pattern = "Plan:",
                                                                 replacement = "<br/><br/> <strong>Plan: </strong>")) %>% 
    dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription,
                                                                 pattern = "Prognosis:",
                                                                 replacement = "<br/><br/> <strong>Prognosis: </strong>")) %>% 
    dplyr::inner_join(cohort %>%
                        dplyr::group_by(.data$phenotypeId) %>%
                        dplyr::summarize(cohortDefinitions = dplyr::n()) %>%
                        dplyr::ungroup(),
                      by = "phenotypeId")
  searchTerms <- getSearchTerms(dataSource = dataSource, includeDescendants = FALSE) %>% 
    dplyr::group_by(.data$phenotypeId) %>%
    dplyr::summarise(searchTermString = paste(.data$term, collapse = ", ")) %>%
    dplyr::ungroup()
  
  phenotypeDescription <- phenotypeDescription %>%
    dplyr::left_join(searchTerms,
                     by = "phenotypeId")
}
 
rm(covariateValue)