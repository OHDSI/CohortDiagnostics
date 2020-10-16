# Code for copying the Shiny app to the ShinyDeploy repo
# Assumes you have cloned both repos, that the current working directory is the root of CohortDiagnostics, and that 
# the ShinyDeploy folder is a sibling of CohortDiagnostics.

sourceFolder <- normalizePath("inst/shiny/DiagnosticsExplorer")
targetFolder <- normalizePath("../ShinyDeploy/PhenotypeLibrary")
 
# Copy app -------------------------------------------------------------------------
if (file.exists(targetFolder)) {  
  unlink(targetFolder, recursive = TRUE)
}
R.utils::copyDirectory(sourceFolder, targetFolder)

# Modify global.R ------------------------------------------------------------------
global <- SqlRender::readSql(file.path(targetFolder, "global.R"))

# Change about text
aboutText <- "
<h3>The OHDSI Phenotype Library</h3>
This app is under development. Please do not use.
"
global <- gsub("defaultAboutText <-.+?\r\n", sprintf("defaultAboutText <- \"%s\"\r\n", aboutText), global)

# Remove characterization tabs:
global <- paste(global, "\r\nrm(covariateValue)")

SqlRender::writeSql(global, file.path(targetFolder, "global.R"))
