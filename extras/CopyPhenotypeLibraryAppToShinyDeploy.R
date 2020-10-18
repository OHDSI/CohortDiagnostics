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
aboutText <- SqlRender::readSql("extras/about/about.html")
global <- gsub("defaultAboutText <-.+?\r\n", 
               sprintf("defaultAboutText <- \"%s\"\r\n", gsub("\"", "\\\\\\\\\"", aboutText)), 
               global)

# Remove characterization tabs:
global <- paste(global, "\r\nrm(covariateValue)")

SqlRender::writeSql(global, file.path(targetFolder, "global.R"))

# Copy image files for about page -------------------------------------------------
wwwDir <- file.path(targetFolder, "www")
dir.create(wwwDir)
file.copy(list.files("extras/about", pattern = ".png", full.names = TRUE), wwwDir)
