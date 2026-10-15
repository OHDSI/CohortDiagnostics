## CRAN Submission: CohortDiagnostics 4.0.0

## Notes for CRAN maintainers

This is the first submission of CohortDiagnostics to CRAN (version 4.0.0). CohortDiagnostics is an R package for the development
and evaluation of phenotype algorithms for OMOP CDM compliant data sets.

### Test environments

- Local macOS (Tahoe 26.4), R 4.5.1
- Ubuntu (via GitHub Actions), R release
- Windows (via GitHub Actions), R release

### R CMD check results

0 ERRORs, 0 WARNINGs, 0 NOTEs

### Reverse dependencies

No declared reverse dependencies.

### Additional notes

- Some packages (ggplot2, plotly, shinydashboard, etc.) are in `Suggests` rather
  than `Imports` because they are only needed for the Shiny Diagnostics Explorer
  app, not for the core package functions. Users who wish to use the Shiny app
  should install these suggested packages.
- Package depends on R >= 4.1.0 due to tidyverse dependency requirements.
- Unit tests use mocked database connections via testthat 3rd edition and
  do not require any external database setup. Integration tests (not run on
  CRAN) require OMOP CDM databases.

### Test results

Unit tests: 537 passed, 0 failed (on macOS with R 4.5.1)
