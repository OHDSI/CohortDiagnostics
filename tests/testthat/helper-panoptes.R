# Helpers for Panoptes-driven database platform tests.

# Existing CDM5_* variables used by earlier integration setups, accepted as
# fallbacks for the normalized HADES_* variables.
panoptesLegacyAliases <- function(dbms) {
  prefix <- switch(dbms,
    "postgresql" = "CDM5_POSTGRESQL",
    "sql server" = "CDM5_SQL_SERVER",
    "oracle" = "CDM5_ORACLE",
    "redshift" = "CDM5_REDSHIFT",
    "spark" = "CDM5_SPARK",
    "snowflake" = "CDM5_SNOWFLAKE",
    "iris" = "CDM5_IRIS",
    NULL
  )
  if (is.null(prefix)) {
    return(NULL)
  }
  list(
    HADES_DB_USER = paste0(prefix, "_USER"),
    HADES_DB_PASSWORD = paste0(prefix, "_PASSWORD"),
    HADES_DB_SERVER = paste0(prefix, "_SERVER"),
    HADES_DB_CONNECTION_STRING = paste0(prefix, "_CONNECTION_STRING"),
    HADES_CDM_SCHEMA = paste0(prefix, "_CDM_SCHEMA"),
    HADES_WORK_SCHEMA = paste0(prefix, "_OHDSI_SCHEMA")
  )
}

# Provisions a local Eunomia database for the sqlite platform so the same
# behavior test runs without an external server. Does nothing for other DBMSs.
localPanoptesSqliteFixture <- function(config, env = parent.frame()) {
  if (!identical(Panoptes::getSelectedTestDbms(config), "sqlite")) {
    return(invisible(NULL))
  }
  if (nzchar(Sys.getenv("HADES_DB_SERVER"))) {
    return(invisible(NULL))
  }
  databaseFile <- tempfile(fileext = ".sqlite")
  Eunomia::getEunomiaConnectionDetails(databaseFile = databaseFile)
  withr::local_envvar(
    c(
      HADES_DB_SERVER = databaseFile,
      HADES_CDM_SCHEMA = "main",
      HADES_WORK_SCHEMA = "main"
    ),
    .local_envir = env
  )
  withr::defer(unlink(databaseFile), envir = env)
  invisible(databaseFile)
}
