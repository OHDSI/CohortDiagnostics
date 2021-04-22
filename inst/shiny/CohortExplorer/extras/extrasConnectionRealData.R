dataSource <- 'truven_ccae'

source(Sys.getenv("startUpScriptLocation")) # this sources information for cdmSources and dataSourceInformation.

connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == dataSource)

dbms <- connectionSpecifications$dbms # example: 'redshift'

port <- connectionSpecifications$port # example: 2234

server <-
  connectionSpecifications$server # example: 'fdsfd.yourdatabase.yourserver.com"

cdmDatabaseSchema <-
  connectionSpecifications$cdmDatabaseSchema # example: "cdm"

vocabDatabaseSchema <-
  connectionSpecifications$vocabDatabaseSchema # example: "vocabulary"

databaseId <-
  connectionSpecifications$database # example: "truven_ccae"

userNameService = "OHDSI_USER" # example: "this is key ring service that securely stores credentials"

passwordService = "OHDSI_PASSWORD" # example: "this is key ring service that securely stores credentials"

cohortDatabaseSchema = paste0('scratch_', keyring::key_get(service = userNameService))

# scratch - usually something like 'scratch_grao'
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = dbms,
  user = keyring::key_get(service = userNameService),
  password = keyring::key_get(service = passwordService),
  port = port,
  server = server
)

cohortTable <-
  paste0("s", connectionSpecifications$sourceId, "_webapi")

cohortDefinitionId = 1

shinySettings <- list(connectionDetails = connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      cohortDatabaseSchema = cohortDatabaseSchema,
                      vocabularyDatabaseSchema = vocabDatabaseSchema,
                      cohortTable = cohortTable,
                      dbms = dbms,
                      cohortDefinitionId = cohortDefinitionId,
                      sampleSize = 10)
