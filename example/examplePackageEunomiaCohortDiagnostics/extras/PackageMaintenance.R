# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of Cohort Diagnostics
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

packageName = "EunomiaCohortDiagnostics"
# remotes::install_github("OHDSI/CohortDiagnostics",
#                         ref = "develop",
#                         dependencies = TRUE)
# remotes::install_github("OHDSI/ROhdsiWebApi", ref = "develop")
library(CohortDiagnostics)

projectDirectory <- file.path(rstudioapi::getActiveProject(), "outputFolder")

##this will delete older files, and rebuild study specifications. Be careful.
unlink(file.path(projectDirectory, 'inst'),
       recursive = TRUE,
       force = TRUE)
dir.create(
  path = file.path(projectDirectory, 'inst', 'settings'),
  recursive = TRUE,
  showWarnings = FALSE
)

baseUrl <- Sys.getenv("BaseUrl")
webApiCohorts <- ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl)
studyCohorts <-  webApiCohorts %>% 
  dplyr::filter(.data$id %in% c(18345,18346,14906,18351,18347,18348,14907,
                                18349,18350,18352,17493,17492,14909,18342,
                                17693,17692,17695,17694,17720, 
                                21402
  ))

cohortsToCreate <- list()
for (i in (1:nrow(studyCohorts))) {
  cohortId <- studyCohorts$id[[i]]
  cohortDefinition <-
    ROhdsiWebApi::getCohortDefinition(cohortId = cohortId, baseUrl = baseUrl)
  if (is.null(cohortDefinition$description)) {
    cohortDefinition$description <- cohortDefinition$name
  }
  df <- tidyr::tibble(
    atlasId = cohortId,
    referentConceptId = 0,
    webApiCohortId = cohortId,
    cohortId = cohortId,
    name = webApiCohortId,
    cohortName = stringr::str_trim(stringr::str_squish(cohortDefinition$name)),
    logicDescription = cohortDefinition$description,
    phenotypeId = 0
  )
  cohortsToCreate[[i]] <- df
}

cohortsToCreate <- dplyr::bind_rows(cohortsToCreate) %>% 
  dplyr::select(-.data$phenotypeId,
                -.data$referentConceptId,
                -.data$logicDescription,
                -.data$name,
                -.data$webApiCohortId)

readr::write_excel_csv(x = cohortsToCreate, na = "", file = "inst/settings/CohortsToCreate.csv", append = FALSE)


# Insert cohort definitions from ATLAS into package -----------------------
ROhdsiWebApi::insertCohortDefinitionSetInPackage(
  fileName = "inst/settings/CohortsToCreate.csv",
  baseUrl = baseUrl,
  insertTableSql = TRUE,
  insertCohortCreationR = TRUE,
  generateStats = TRUE,
  packageName = packageName
)

