getPredictionHelp <- function(file){
  fileLoc <- system.file(
    'patient-level-prediction-www', 
    file, 
    package = "CohortDiagnostics"
  )
  return(fileLoc)
}
