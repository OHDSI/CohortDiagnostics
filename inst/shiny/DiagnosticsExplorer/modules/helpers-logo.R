#' The location of the OHDSI logo
#'
#' @details
#' Returns the location of the OHDSI logo
#' 
#' @return
#' string location of the OHDSI logo
#'
#' @family Utils
#' @export
getLogoImage <- function(){
  fileLoc <- system.file(
    'images', 
    "logo.png", 
    package = "OhdsiShinyModules"
  )
  return(fileLoc)
}
