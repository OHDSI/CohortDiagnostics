


#' Internal function to check arguments used throughout the CohortDiagnostics package
#'
#' @param arg The argument to check
#' @param add An optional checkmate::makeAssertCollection() to add error messages to
#'
#' @return NULL
checkArg <- function(arg, add) {
  
  argName <- deparse(substitute(arg))
  
  if (argName == "connection") {
    checkmate::assertClass(arg, "DatabaseConnectorConnection")
    checkmate::assertTRUE(DatabaseConnector::dbIsValid(connection))
    
  } else if (argName %in% c("tempEmulationSchema", "cdmDatabaseSchema", "vocabularyDatabaseSchema", "cohortDatabaseSchema")) {
    checkmate::assertCharacter(arg, len = 1, pattern = "^[A-Za-z][A-Za-z0-9_]*$", null.ok = TRUE)
    
  } else if (argName == "cohortTableNames") {
    checkmate::assertList(arg, null.ok = FALSE, types = "character", add = add, names = "named")
    checkmate::assertNames(names(arg),
                           must.include = c(
                             "cohortTable",
                             "cohortInclusionTable",
                             "cohortInclusionResultTable",
                             "cohortInclusionStatsTable",
                             "cohortSummaryStatsTable",
                             "cohortCensorStatsTable"
                           ),
                           add = add
    )
  } else if (argName == "cohortDefinitionSet") {
    checkmate::assertTRUE(CohortGenerator::isCohortDefinitionSet(arg))
    checkmate::assertDataFrame(arg, add = add)
    checkmate::assertNames(names(arg),
                           must.include = c(
                             "json",
                             "cohortId",
                             "cohortName",
                             "sql"
                           ),
                           add = add
    )
    
  } else if (argName == "cohortIds") {
    checkmate::assertIntegerish(arg, lower = 0, any.missing = FALSE, null.ok = TRUE, add = add)
    
  } else if (argName == "minCellCount") {
    arg <- utils::type.convert(arg, as.is = TRUE)
    checkmate::assertInteger(x = arg, len = 1, lower = 0, add = add)
    
  } else if (argName == "minCharacterizationMean") {
    arg <- utils::type.convert(arg, as.is = TRUE)
    checkmate::assertNumeric(x = arg, lower = 0, add = add, any.missing = FALSE)
    
  } else if (argName == "incremental") {
    checkmate::assertLogical(arg, add = add, len = 1, add = add, any.missing = FALSE)
    
  } else if (argName == "cohortTable") {
    checkmate::assertCharacter(x = arg, min.len = 1, pattern = "^[A-Za-z][A-Za-z0-9_]*$", add = add, any.missing = FALSE)
  
  } else if (argName == "databaseId") {
    checkmate::assertCharacter(x = arg, min.len = 1, add = add)
  
  } else if (argName == "exportFolder") {
    checkmate::assertCharacter(arg, len = 1, any.missing = FALSE)
    if (!file.exists(gsub("/$", "", arg))) {
      dir.create(arg, recursive = TRUE)
      ParallelLogger::logInfo("Created export folder", arg)
    }
    checkmate::assertDirectory(arg, access = "w", add = add)
    
  } else if (argName == "incrementalFolder") {
    checkmate::assertCharacter(arg, len = 1, any.missing = FALSE)
    if (!file.exists(gsub("/$", "", arg))) {
      dir.create(arg, recursive = TRUE)
      ParallelLogger::logInfo("Created incremental folder", arg)
    }
    checkmate::assertDirectory(arg, access = "w", add = add)
    
  } else if (argName == "temporalCovariateSettings") {
    if (is(arg, "covariateSettings")) {
      arg <- list(arg)
    }
    checkmate::assert_true(all(lapply(arg, class) == c("covariateSettings")), add = add)
    
    requiredCharacterisationSettings <- c(
      "DemographicsGender", "DemographicsAgeGroup", "DemographicsRace",
      "DemographicsEthnicity", "DemographicsIndexYear", "DemographicsIndexMonth",
      "ConditionEraGroupOverlap", "DrugEraGroupOverlap", "CharlsonIndex",
      "Chads2", "Chads2Vasc"
    )
    presentSettings <- arg[[1]][requiredCharacterisationSettings]
    if (!all(unlist(presentSettings))) {
      warning(
        "For cohort characterization to display standardized results, the following covariates must be present in your temporalCovariateSettings: \n\n",
        paste(requiredCharacterisationSettings, collapse = ", ")
      )
    }
    
    requiredTimeDistributionSettings <- c(
      "DemographicsPriorObservationTime",
      "DemographicsPostObservationTime",
      "DemographicsTimeInCohort"
    )
    presentSettings <- arg[[1]][requiredTimeDistributionSettings]
    if (!all(unlist(presentSettings))) {
      warning(
        "For time distributions diagnostics to display standardized results, the following covariates must be present in your temporalCovariateSettings: \n\n",
        paste(requiredTimeDistributionSettings, collapse = ", ")
      )
    }
    
    checkmate::assert_integerish(
      x = arg[[1]]$temporalStartDays,
      any.missing = FALSE,
      min.len = 1,
      add = add
    )
    checkmate::assert_integerish(
      x = arg[[1]]$temporalEndDays,
      any.missing = FALSE,
      min.len = 1,
      add = add
    )
  } else {
    stop(paste("Argument", argName, "was not checked! Add the check to checkArg() or perform the check outside of checkArg()."))
  }
  return(invisible(NULL))
}
