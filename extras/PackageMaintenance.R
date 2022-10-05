# @file PackageMaintenance
#
# Copyright 2022 Observational Health Data Sciences and Informatics
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

# Format and check code
OhdsiRTools::checkUsagePackage("CohortDiagnostics")
OhdsiRTools::updateCopyrightYearFolder()
styler::style_pkg()
devtools::spell_check()
spelling::spell_check_files(list.files(path = "inst/shiny", pattern = "*.html", recursive = TRUE, full.names = TRUE))


# Create manual and vignettes:
unlink("extras/CohortDiagnostics.pdf")
system("R CMD Rd2pdf ./ --output=extras/CohortDiagnostics.pdf")

dir.create(path = "./inst/doc/", showWarnings = FALSE)


rmarkdown::render("vignettes/ViewingResultsUsingDiagnosticsExplorer.Rmd",
                  output_file = "../inst/doc/ViewingResultsUsingDiagnosticsExplorer.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))


rmarkdown::render("vignettes/WhatIsCohortDiagnostics.Rmd",
                  output_file = "../inst/doc/WhatIsCohortDiagnostics.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

rmarkdown::render("vignettes/RunningCohortDiagnostics.Rmd",
                  output_file = "../inst/doc/RunningCohortDiagnostics.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

pkgdown::build_site()
OhdsiRTools::fixHadesLogo()

# Change shiny app version number to current version
pattern <- "Version: (\\d+\\.\\d+\\.\\d+)"
text <- readChar("DESCRIPTION", file.info("DESCRIPTION")$size)
version <- stringi::stri_extract(text, regex = pattern)

filePath <- file.path("inst", "shiny", "DiagnosticsExplorer", "global.R")
text <- readChar(filePath, file.info(filePath)$size)
patternRep <- 'appVersionNum <- "Version: (\\d+\\.\\d+\\.\\d+)"'
text <- gsub(patternRep, paste0('appVersionNum <- "', version, '"'), text)
writeLines(text, con = file(filePath))


version <- gsub("Version: ", "", version)
filePath <- file.path("inst", "sql", "sql_server", "CreateResultsDataModel.sql")
text <- readChar(filePath, file.info(filePath)$size)
patternRep <- "\\{DEFAULT @version_number = '(\\d+\\.\\d+\\.\\d+)'\\}"# 'appVersionNum <- "Version: (\\d+\\.\\d+\\.\\d+)"'
text <- gsub(patternRep, paste0("\\{DEFAULT @version_number = '", version, "'\\}"), text)
writeLines(text, con = file(filePath))


# Copy data model specs to Shiny app
file.copy(from = "inst/settings/resultsDataModelSpecification.csv", 
          to = "inst/shiny/DiagnosticsExplorer/data/resultsDataModelSpecification.csv",
          overwrite = TRUE)


# Copy shared script
file.copy(from = "R/Shared.R", 
          to = "inst/shiny/DiagnosticsExplorer/R/Shared.R",
          overwrite = TRUE)


# Cohort Analysis Ref used by Shiny to report CohortRelationship
cohortAnalysisRef <-
  dplyr::bind_rows(
    dplyr::tibble(
      analysisName = c(
        "c1:precedes (p)",
        "c1:meets (m)",
        "c1:overlaps (o)",
        "c1:finished by (F)",
        "c1:contains (D)",
        "c1:starts (s)",
        "c1:equals (e)",
        "c1:started by (S)",
        "c1:during (d)",
        "c1:finishes (f)",
        "c1:overlapped by (O)",
        "c1:met by (M)",
        "c1:preceded by (P)"
      ),
      analysisId = c(-1:-13)
    ),
    dplyr::tibble(
      analysisName = c(
        "c2:endsIn (osd)",
        "c2:startsWithStart (seS)",
        "c2:startsIn (dfO)",
        "c2:endsWithEnd (Fef)",
        "c2:startsBeforeStart (pmoFD)",
        "c2:startsAfterStart (dfOMP)",
        "c2:startsBeforeEnd",
        "c2:endsBeforeEnd (pmoFDseSd)",
        "c2:endsAfterEnd (DSOMP)",
        "c2:startsInInclusive (seSdfOM)",
        "c2:endsInInclusive (oFsedf)",
        "c2:startsOnOrBeforeStart (pmoFDseS)",
        "c2:startsOnOrBeforeEnd (pmoFDseSdfoM)",
        "c2:endsOnOrBeforeEnd (pmoFsedf)",
        "c2:duringInclusive (esdf)"
      ),
      analysisId = c(-101:-115)
    )
  ) %>%
  dplyr::mutate(domainId = "Cohort",
                isBinary = "Y",
                missingMeansZero = "Y") %>% 
  dplyr::arrange(dplyr::desc(.data$analysisId))


readr::write_excel_csv(
  x = cohortAnalysisRef,
  file = "inst/shiny/DiagnosticsExplorer/data/cohortAnalysisRef.csv",
  na = "",
  append = FALSE,
  quote = "all"
)
