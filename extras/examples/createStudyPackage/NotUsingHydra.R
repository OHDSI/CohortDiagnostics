################# UNTESTED ###############################


#remotes::install_github("OHDSI/Hydra")
outputFolder <-
  "d:/temp/output"  # location where you study package will be created


########## Please populate the information below #####################
version <- "v0.1.0"
name <-
  "Thrombosis With Thrombocytopenia Syndrome cohorts - an OHDSI network study"
packageName <- "ThrombosisWithThrombocytopeniaSyndrome"
skeletonVersion <- "v0.0.1"
createdBy <- "rao@ohdsi.org"
createdDate <- Sys.Date() # default
modifiedBy <- "rao@ohdsi.org"
modifiedDate <- Sys.Date()
skeletonType <- "CohortDiagnosticsStudy"
organizationName <- "OHDSI"
description <-
  "Cohort diagnostics on Thrombosis With Thrombocytopenia Syndrome cohorts."


library(magrittr)
# Set up
baseUrl <- Sys.getenv("baseUrlUnsecure")
cohortIds <- c(
  22040,
  22042,
  22041,
  22039,
  22038,
  22037,
  22036,
  22035,
  22034,
  22033,
  22031,
  22032,
  22030,
  22028,
  22029
)



################# end of user input ##############
webApiCohorts <-
  ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl)
studyCohorts <-  webApiCohorts %>%
  dplyr::filter(.data$id %in% cohortIds)

# compile them into a data table
cohortDefinitionsArray <- list()
for (i in (1:nrow(studyCohorts))) {
  cohortDefinition <-
    ROhdsiWebApi::getCohortDefinition(cohortId = studyCohorts$id[[i]],
                                      baseUrl = baseUrl)
  cohortDefinitionsArray[[i]] <- list(
    id = studyCohorts$id[[i]],
    createdDate = studyCohorts$createdDate[[i]],
    modifiedDate = studyCohorts$createdDate[[i]],
    logicDescription = studyCohorts$description[[i]],
    name = stringr::str_trim(stringr::str_squish(cohortDefinition$name)),
    expression = cohortDefinition$expression
  )
}

tempFolder <- tempdir()
unlink(x = tempFolder,
       recursive = TRUE,
       force = TRUE)
dir.create(path = tempFolder,
           showWarnings = FALSE,
           recursive = TRUE)

specifications <- list(
  id = 1,
  version = version,
  name = name,
  packageName = packageName,
  skeletonVersin = skeletonVersion,
  createdBy = createdBy,
  createdDate = createdDate,
  modifiedBy = modifiedBy,
  modifiedDate = modifiedDate,
  skeletontype = skeletonType,
  organizationName = organizationName,
  description = description,
  cohortDefinitions = cohortDefinitionsArray
)


# download package from github

# download a .zip file of the repository
# from the "Clone or download - Download ZIP" button
# on the GitHub repository of interest
downloadSkeleton <- function(outputFolder,
                             packageName,
                             skeletonType = 'SkeletonCohortDiagnosticsStudy') {
  # check outputFolder exists
  
  # check file.path(outputFolder,  packageName) does not exist
  
  # download, unzip and rename:
  
  download.file(
    url = paste0(
      "https://github.com/ohdsi/",
      skeletonType,
      "/archive/master.zip"
    )
    ,
    destfile = file.path(outputFolder, "package.zip")
  )
  # unzip the .zip file
  unzip(zipfile = file.path(outputFolder, "package.zip"),
        exdir = outputFolder)
  file.rename(
    from = file.path(outputFolder, paste0(skeletonType, '-master')),
    to = file.path(outputFolder,  packageName)
  )
  unlink(file.path(outputFolder, "package.zip"))
}

downloadSkeleton(outputFolder = 'D:/temp/shinyTest',
                 packageName = 'pheMyocardialInfarction')

# change name
replaceName <- function(packageLocation = getwd(),
                        packageName = 'pheMyocardialInfarction',
                        skeletonType = 'SkeletonCohortDiagnosticsStudy') {
  filesToRename <-
    c(paste0(skeletonType, ".Rproj"),
      paste0("R/", skeletonType, ".R"))
  for (f in filesToRename) {
    ParallelLogger::logInfo(paste0('Renaming ', f))
    fnew <- gsub(skeletonType, packageName, f)
    file.rename(from = file.path(packageLocation, f),
                to = file.path(packageLocation, fnew))
  }
  
  filesToEdit <- c(
    file.path(packageLocation, "DESCRIPTION"),
    file.path(packageLocation, "README.md"),
    file.path(packageLocation, "extras/CodeToRun.R"),
    dir(file.path(packageLocation, "R"), full.names = T)
  )
  for (f in filesToEdit) {
    ParallelLogger::logInfo(paste0('Editing ', f))
    x <- readLines(f)
    y <- gsub(skeletonType, packageName, x)
    cat(y, file = f, sep = "\n")
  }
}

replaceName(packageLocation = file.path(outputFolder,  packageName),
            packageName = packageName)


# save json file into isnt/settings/specifications.json
saveAnalysisJson <- function(packageLocation,
                             analysisList) {
  write(
    RJSONIO::toJSON(analysisList, digits = 23),
    file = file.path(packageLocation, 'inst', 'settings', 'specifications.json')
  )
}

saveAnalysisJson(packageLocation = file.path(outputFolder,  packageName),
                 analysisList = specifications)

# create cohorts to create from cohortDefinitions
# save json and convert+save sql into inst/cohorts and inst/sql/sql_server
saveCohorts <- function(packageLocation,
                        analysisList,
                        baseUrl) {
  nameForFile <- function(name) {
    writeLines(name)
    name <- gsub(' ', '', name)
    name <- gsub("[[:punct:]]", "_", name)
    writeLines(name)
    return(name)
  }
  
  details <-
    lapply(1:length(analysisList$cohortDefinitions), function(i)
      c(
        name = analysisList$cohortDefinitions[[i]]$name,
        cohortId = analysisList$cohortDefinitions[[i]]$id,
        atlasId = analysisList$cohortDefinitions[[i]]$id
      ))
  details <- do.call('rbind', details)
  details <- as.data.frame(details, stringsAsFactors = F)
  details$name <- nameForFile(details$name)
  
  write.csv(
    x = details,
    file = file.path(packageLocation, 'inst', 'settings', 'cohortsToCreate.csv'),
    row.names = F
  )
  
  # make sure cohorts and sql/sql_server exist
  if (!dir.exists(file.path(packageLocation, 'inst', 'cohorts'))) {
    dir.create(file.path(packageLocation, 'inst', 'cohorts'), recursive = T)
  }
  if (!dir.exists(file.path(packageLocation, 'inst', 'sql', 'sql_server'))) {
    dir.create(file.path(packageLocation, 'inst', 'sql', 'sql_server'),
               recursive = T)
  }
  
  # save the cohorts as json
  lapply(1:length(analysisList$cohortDefinitions), function(i) {
    write(
      RJSONIO::toJSON(analysisList$cohortDefinitions[[i]], digits = 23),
      file = file.path(
        packageLocation,
        'inst',
        'cohorts',
        paste0(
          nameForFile(analysisList$cohortDefinitions[[i]]$name),
          '.json'
        )
      )
    )
  })
  
  # save the cohorts as sql
  lapply(1:length(analysisList$cohortDefinitions), function(i) {
    write(
      ROhdsiWebApi::getCohortSql(analysisList$cohortDefinitions[[i]], baseUrl = baseUrl),
      file = file.path(
        packageLocation,
        'inst',
        'sql',
        'sql_server',
        paste0(
          nameForFile(analysisList$cohortDefinitions[[i]]$name),
          '.sql'
        )
      )
    )
  })
  
}
specifications$cohortDefinitions[[3]] <- NULL
saveCohorts(
  packageLocation = file.path(outputFolder,  packageName),
  analysisList = specifications,
  baseUrl = 'http://api.ohdsi.org:8080/WebAPI'
)
