getPredictionHelp <- function(file){
  fileLoc <- system.file(
    'patient-level-prediction-www', 
    file, 
    package = "OhdsiShinyModules"
  )
  return(fileLoc)
}
