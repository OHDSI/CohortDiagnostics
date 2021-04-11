# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of examplePackage
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

library(magrittr)
packageName = "examplePackage"


##### use this code if you would like to import cohort definitions from Atlas. This code will generate
##### cohortDiagnostics specifications using ROhdsiWebApi
# packageDirectory <- rstudioapi::getActiveProject()
# unlink(file.path(dir, 'inst', 'settings'))
# dir.create(path = file.path(dir, 'inst', 'settings'), recursive = TRUE, showWarnings = FALSE)
# atlasCohortId <- c() # enter your Altas cohort ids
# baseUrl <- "" # enter the base Url of your Atlas webapi instance. Note this does not work on security enabled Atlas.
# cohortsToCreate <- list()
# for (i in (1:length(atlasCohortId))) {
#   df <- tidyr::tibble(atlasId = atlasCohortId[[i]],
#                       referentConceptId = 0,
#                       webApiCohortId = atlasCohortId[[i]], 
#                       cohortId = atlasCohortId[[i]], 
#                       ROhdsiWebApi::getCohortDefinition(cohortId = atlasCohortId[[i]],
#                                                         baseUrl = baseUrl)$name)),
#                       cohortName = stringr::str_trim(stringr::str_squish(
#                       ROhdsiWebApi::getCohortDefinition(cohortId = atlasCohortId[[i]],
#                                                         baseUrl = baseUrl)$name)),
#                       logicDescription = 'None provided')
#   cohortsToCreate[[i]] <- df
# }
# cohortsToCreate <- dplyr::bind_rows(cohortsToCreate)
# readr::write_excel_csv(x = cohortsToCreate, file = "inst/settings/CohortsToCreate.csv")
# #### Insert cohort definitions from ATLAS into package -----------------------
# ROhdsiWebApi::insertCohortDefinitionSetInPackage(fileName = "inst/settings/CohortsToCreate.csv",
#                                                  baseUrl = baseUrl,
#                                                  insertTableSql = TRUE,
#                                                  insertCohortCreationR = TRUE,
#                                                  generateStats = TRUE,
#                                                  packageName = packageName)

# Format and check code ---------------------------------------------------
OhdsiRTools::formatRFolder()
OhdsiRTools::checkUsagePackage(packageName)
OhdsiRTools::updateCopyrightYearFolder()

# Create manual -----------------------------------------------------------
shell("rm extras/examplePackage.pdf")
shell("R CMD Rd2pdf ./ --output=extras/examplePackage.pdf")

# Store environment in which the study was executed -----------------------
OhdsiRTools::insertEnvironmentSnapshotInPackage(packageName)
