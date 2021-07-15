detach("package:CohortDiagnostics", unload=TRUE)
covResults <- covr::package_coverage()
covr::report(covResults)
