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
This can be changed to use a different database engine.

## Configuration using other 
The default YAML file contains the following options
```{yaml}
# Alter these configuration settings for usage with remote databases
connectionDetails:
  dbms: "sqlite"
  server: "data/MergedCohortDiagnosticsData.sqlite"
  user: ""
  password: ""
  port: ""

# Store connection details as a json string in keyring
# Store with keyring::key_set_with_value("KEYNAME", jsonlite::toJSON(myConnectionDetails))
connectionDetailsSecureKey: ~

# If you used a table prefix to store the results (e.g. cd_)
tablePrefix: ""

# Results schema being used
resultsDatabaseSchema: "main"

# Vocabulary schemas. Should include the reuslts schema and optional custom vocabularies
vocabularyDatabaseSchemas: ["main"]

# Custom table name for cohorts and cdm databases (probably not needed, ignores table prefix)
cohortTableName: "cohort"
databaseTableName: "database"

# If you wish to enable annotation - not currently reccomended in multi-user environments
enableAnnotation: TRUE
enableAuthorization: TRUE

### if you need a way to authorize users
### generate hash using code like digest::digest("diagnostics",algo = "sha512")
### store in external file called UserCredentials.csv - with fields userId, hashCode
### place the file in the root folder
userCredentialsFile: UserCredentials.csv
```

### Setting connection details

Connection details should conform to a standard `DatabaseConnector` connection details object, in yaml format.
Optionally, this object can be saved to a secure secret using the keyring package as follows:

```{r}
myConnectionDetails <- list(
    user = "myusername",
    password = "someSecretPassword",
    server = "my-server-address.com/cohort_diagnostics",
    dbms = "postgresql",
    port = 5432
)
keyring::key_set_with_value("KEYNAME", jsonlite::toJSON(myConnectionDetails))
```

This list will then be loaded at runtime, meaning that secure credentails don't need to be saved in plain
text.

## OHDSI shiny server instructions
To use this shiny app on the (OHDSI ShinyServer)['https://github.com/OHDSI/ShinyDeploy'] use the environment variable
configuration settings included in the file `config-ohdsi-shiny.yml`.
The following settings can be reconfigured if environment variables differ:

```{yaml}
# Alter these configuration settings for usage with remote databases


# store connection details with environment variables
# Note - if dbms and port vars are unset in environment variables they will default to above connectionDetails settings
# See above for postgresql 
connectionEnvironmentVariables:
  dbms: ~
  database: "shinydbDatabase"
  server: "shinydbServer"
  user: "shinydbUser"
  password: "shinydbPw"
  port: "shinydbPort"
  extraSettings: ~

connectionDetails:
  dbms: "postgresql"
  port: 5432

# This must be set up by a system administrator
resultsDatabaseSchema: "some_schema"
vocabularyDatabaseSchemas: ["some_schema"]
```


This file makes use of system set environment variables with a read-only account.
Please contact the system administrator for creating a schema that can be accessed by the applications.