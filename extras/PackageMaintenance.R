# @file PackageMaintenance
#
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

# Format and check code
# OhdsiRTools::formatRFolder() (note: this function has been impacted by change in formatR)
OhdsiRTools::checkUsagePackage("CohortDiagnostics")
OhdsiRTools::updateCopyrightYearFolder()
devtools::spell_check()
spelling::spell_check_files(list.files(path = "inst/shiny", pattern = "*.html", recursive = TRUE, full.names = TRUE))


# Create manual and vignettes:
unlink("extras/CohortDiagnostics.pdf")
shell("R CMD Rd2pdf ./ --output=extras/CohortDiagnostics.pdf")

dir.create(path = "./inst/doc/", showWarnings = FALSE)
rmarkdown::render("vignettes/CohortDiagnosticsUsingWebApi.Rmd",
                  output_file = "../inst/doc/CohortDiagnosticsUsingWebApi.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

pkgdown::build_site()
OhdsiRTools::fixHadesLogo()

# Regenerate DDL - doesnt work. Maintain manually
# pathToCsv <- file.path("inst", "settings", "resultsDataModelSpecification.csv")
# specifications <- readr::read_csv(file = pathToCsv, col_types = readr::cols())
# source("extras/ResultsDataModel.R")
# createDdl("inst/sql/postgresql/CreateResultsDataModel.sql", specifications)

# Copy data model specs to Shiny app
file.copy(from = "inst/settings/resultsDataModelSpecification.csv", 
          to = "inst/shiny/DiagnosticsExplorer/resultsDataModelSpecification.csv",
          overwrite = TRUE)
