exampleStudy
============

This study package demonstrates how to currently use the CohortDiagnostics package in a study. The study package was generated automatically by ATLAS, after which the following changes were made:

1. Modified extras/PackageMaintenance.R to use the ROhdsiWebApi package, and to generate inclusion rule statistics when creating cohorts.
2. Ran the function modified in step 1 to regenerate the cohort definitions, SQL, and R code.
3. Added a dependency to CohortDiagnostics to DESCRIPTION
4. Added the file R/Feasibility.R, which defines the runFeasibility function.

Don't forget to build the package before running!