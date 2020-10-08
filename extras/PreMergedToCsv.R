outputMergedCsvFiles <- function(preMergedFile = "PreMerged.RData", outputPath = ".") {
  e <- new.env(parent = emptyenv())
  load(preMergedFile, envir = e)
  objs <- ls(envir = e, all.names = TRUE)
  for (obj in objs) {
    .x <- get(obj, envir = e)
    
    for (name in names(.x)) {
      colnames(.x)[colnames(.x) == name] <- SqlRender::camelCaseToSnakeCase(name)
    }
    
    outputFile <- paste0(SqlRender::camelCaseToSnakeCase(obj), ".csv")
    message(sprintf('Saving %s as %s', obj, outputFile))
    readr::write_excel_csv(x = .x, file = file.path(outputPath, outputFile), na = "")
  }
}

outputMergedCsvFiles(preMergedFile = "output/PreMerged.RData", outputPath = "output/")
