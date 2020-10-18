# Code to be used  by the package maintainer.

# Format and check code ---------------------------------------------------------------------
OhdsiRTools::formatRFolder()
OhdsiRTools::checkUsagePackage("SkeletonCohortDiagnostics")
OhdsiRTools::updateCopyrightYearFolder()

# Create manual -----------------------------------------------------------------------------
shell("rm extras/SkeletonCohortDiagnostics.pdf")
shell("R CMD Rd2pdf ./ --output=extras/SkeletonCohortDiagnostics.pdf")

# Import phenotype and cohort definitions from Phenotype Library ----------------------------
source("extras/ImportFromPhenotypeLibrary.R")
importFromPhenotypeLibrary("../PhenotypeLibrary")

# Store R environment details in RENV lock file ---------------------------------------------------
OhdsiRTools::createRenvLockFile("SkeletonCohortDiagnostics")
