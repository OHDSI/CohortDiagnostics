# Diagnostics Explorer Shiny App
The DiagnosticsExplorer Shiny application is designed to be the primary method for exploring CohortDiagnostics results.
See the [CohortDiagnostics documentation](https://ohdsi.github.io/CohortDiagnostics/index.html) for guidance on running CohortDiagnostics and generating the required results.

This application directory is designed to be a stand-alone shiny application.
To install the required packages for this app we recommend using `renv` to create an isolated environment. 
Assuming your working directory is the DiagnosticsExplorer shiny app, run the following:

```{r}
install.packages("renv")
renv::restore()
```

The shiny app can now be launched:

```
shiny::runApp()
```

The default data is file is intended to be placed in `data/MergedCohortDiagnosticsData.sqlite`.