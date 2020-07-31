# @file PackageMaintenance
#
# Copyright 2020 Observational Health Data Sciences and Informatics
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

# Format and check codeP
# OhdsiRTools::formatRFolder() (note: this function has been impacted by change in formatR)
OhdsiRTools::checkUsagePackage("CohortDiagnostics")
OhdsiRTools::updateCopyrightYearFolder()
devtools::spell_check()
spelling::spell_check_files(list.files(path = "inst/shiny", pattern = "*.html", recursive = TRUE, full.names = TRUE))


# Create manual and vignettes:
unlink("extras/CohortDiagnostics.pdf")
shell("R CMD Rd2pdf ./ --output=extras/CohortDiagnostics.pdf")

rmarkdown::render("vignettes/CohortDiagnosticsUsingWebApi.Rmd",
                  output_file = "../inst/doc/CohortDiagnosticsUsingWebApi.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

pkgdown::build_site()
OhdsiRTools::fixHadesLogo()

# Install cohorts for testing
ROhdsiWebApi::insertCohortDefinitionSetInPackage(fileName = "inst/settings/CohortsToCreateForTesting.csv",
                                                 baseUrl = Sys.getenv("baseUrl"),
                                                 insertTableSql = FALSE,
                                                 generateStats = TRUE,
                                                 insertCohortCreationR = FALSE,
                                                 packageName = "CohortDiagnostics")

