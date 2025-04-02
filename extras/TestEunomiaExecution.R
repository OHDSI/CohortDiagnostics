## For developer convenience to create small test data sets

devtools::load_all()

cohortDefinitionSet <- loadTestCohortDefinitionSet()
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

gitHeadSha <- system("git rev-parse --short HEAD", intern = TRUE)
resFile <- glue::glue("tests/testthat/test_datasets/cd-test-dataset-{gitHeadSha}.sqlite")

cohortTable <- "cohort"
vocabularyDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cdmDatabaseSchema <- "main"

createTestShinyDb(connectionDetails = connectionDetails,
                  outputPath = resFile,
                  cohortTable = "cohort",
                  vocabularyDatabaseSchema = "main",
                  cohortDatabaseSchema = "main",
                  cdmDatabaseSchema = "main",
                  cohortDefinitionSet = cohortDefinitionSet)