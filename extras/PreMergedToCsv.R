outputMergedCsvFiles <- function(preMergedFile = "PreMerged.RData", outputPath = ".") {
  e <- new.env(parent = emptyenv())
  load(preMergedFile, envir = e)
  objs <- ls(envir = e, all.names = TRUE)
  for (obj in objs) {
    .x <- get(obj, envir = e)
    outputFile <- paste0(SqlRender::camelCaseToSnakeCase(obj), ".csv")
    message(sprintf('Saving %s as %s', obj, outputFile))
    write.csv(.x, file = file.path(outputPath, outputFile), row.names = FALSE)
  }
}

outputMergedCsvFiles(preMergedFile = "\\\\AWSASJNVA4217\\output\\PreMerged.RData", outputPath = "\\\\AWSASJNVA4217\\output\\merged_csvs")