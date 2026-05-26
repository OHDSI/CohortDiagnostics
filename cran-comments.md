## CRAN Submission: CohortDiagnostics 3.4.2

## Notes for CRAN maintainers

This is a resubmission of CohortDiagnostics, an R package for the development
and evaluation of phenotype algorithms for OMOP CDM compliant data sets.

### Test environments

- Local macOS (Sequoia 15.6), R 4.5.1
- Ubuntu (via GitHub Actions), R release
- Windows (via GitHub Actions), R release

### R CMD check results

0 ERRORs, 0 WARNINGs, 0 NOTEs

(When vignettes are built: 0 ERRORs, 0 WARNINGs, 0 NOTEs)

### Reverse dependencies

No declared reverse dependencies.

### Additional notes

- `StagedInstall: no` is set because the package includes a Shiny application
  with JavaScript/CSS dependencies that need to be available at install time.
- Some packages (ggplot2, plotly, shinydashboard, etc.) are in `Suggests` rather
  than `Imports` because they are only needed for the Shiny Diagnostics Explorer
  app, not for the core package functions. Users who wish to use the Shiny app
  should install these suggested packages.
- The `lib/` directory contains JavaScript/CSS libraries used by the Shiny
  diagnostics explorer and is excluded from the built package via `.Rbuildignore`.
- Package depends on R >= 4.1.0 due to tidyverse dependency requirements.
- Unit tests use mocked database connections and do not require any external
  database setup. Integration tests (not run on CRAN) require OMOP CDM databases.

### Test results

Unit tests: 537 passed, 0 failed (on macOS with R 4.5.1)
