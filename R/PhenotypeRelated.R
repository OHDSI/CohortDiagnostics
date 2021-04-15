# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
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


loadAndExportPhenotypeDescription <- function(packageName,
                                              phenotypeDescriptionFile,
                                              exportFolder,
                                              cohorts, 
                                              errorMessage = NULL) {
  if (is.null(errorMessage)) {
    errorMessage <- checkmate::makeAssertCollection(errorMessage)
  }
  pathToCsv <- system.file("settings", phenotypeDescriptionFile, package = packageName)
  if (file.exists(pathToCsv)) {
    ParallelLogger::logInfo("Found phenotype description file. Loading.")
    
    checkInputFileEncoding(pathToCsv)
    
    phenotypeDescription <- readr::read_csv(file = pathToCsv, 
                                            col_types = readr::cols(),
                                            na = 'NA',
                                            guess_max = min(1e7)) %>% 
      dplyr::arrange(.data$phenotypeName, .data$phenotypeId)
    
    checkmate::assertTibble(x = phenotypeDescription, 
                            any.missing = TRUE, 
                            min.rows = 1, 
                            min.cols = 3, 
                            add = errorMessage)
    checkmate::assertNames(x = colnames(phenotypeDescription),
                           must.include = c("phenotypeId", "phenotypeName","clinicalDescription"),
                           add = errorMessage)
    checkmate::reportAssertions(collection = errorMessage)
    
    phenotypeDescription <- phenotypeDescription %>% 
      dplyr::select(.data$phenotypeId, .data$phenotypeName, .data$clinicalDescription) %>% 
      dplyr::mutate(phenotypeName = dplyr::coalesce(as.character(.data$phenotypeName),""),
                    clinicalDescription = dplyr::coalesce(as.character(.data$clinicalDescription),"")
      )  
    checkmate::assertTibble(x = phenotypeDescription,
                            types = c("double", "character"), add = errorMessage)
    checkmate::reportAssertions(collection = errorMessage)
    
    ParallelLogger::logInfo(sprintf("Phenotype description file has %s rows. Matching with submitted cohorts", 
                                    nrow(phenotypeDescription)))
    
    phenotypeDescription <- phenotypeDescription %>% 
      dplyr::filter(.data$phenotypeId %in% unique(cohorts$phenotypeId))
    
    ParallelLogger::logInfo(sprintf("%s rows matched", nrow(phenotypeDescription)))
    
    if (nrow(phenotypeDescription) > 0) {
      writeToCsv(phenotypeDescription, file.path(exportFolder, "phenotype_description.csv"))
    } else {
      warning("Phentoype description csv file found, but records dont match the referent concept ids of the cohorts being diagnosed.")
    }
    return(phenotypeDescription)
  } else {
    warning("Phentoype description file not found")
    return(NULL)
  }
}
