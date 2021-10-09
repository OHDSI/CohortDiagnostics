CohortDiagnostics 2.1.3
=======================

Changes:
1. On starting diagnostics explorer using launchDiagnosticsExplorer - checks were added for remotes and CirceR https://github.com/OHDSI/CohortDiagnostics/issues/595

Bug Fixes:
1. Diagnostics explorer - characterization plot would show a warning message when one of the cohorts selected has no data. Added check for this issue.

CohortDiagnostics 2.1.2
=======================

Bug fixes:
1. DiagnosticsExplorer fixes a bug of app failure when runIndexEventBreakdown, runOrphanConcepts, includedSourceConcept is set to FALSE


CohortDiagnostics 2.1.1
=======================

Bug fixes:

1. DiagnosticsExplorer shiny app bug fixes: inclusion rule display. 
2. Handle situation where cdm_source may have more than one row record. warning + use max value for vocabularyVersion.
3. Switch off runTimeSeries for BigQuery because of SQL translation issue, to be addressed in version 2.3. https://github.com/OHDSI/CohortDiagnostics/issues/503 Also runTimeSeries is set to FALSE by default.
3. Fixed a bug in DDL - temporal_covariate_value_dist https://github.com/OHDSI/CohortDiagnostics/issues/490
See issue https://github.com/OHDSI/CohortDiagnostics/issues/497


CohortDiagnostics 2.1.0
=======================

Changes:

1. Diagnostics explorer Shiny app enhancements: 
- Improved tool tip
- Various improvements to plots for consistent color, axis labels and labels
- Visit context table addition
- Diagnostic explorer is now a distinct shiny application from phenotype library. PhenotypeExplorer is a stand alone shiny app in package PhenotypeLibrarian.
- Lot of UX changes. Reactivity deferred on drop down menus.
- Changes to improve app stability.
2. Index event breakdown now has subject count
3. Index event breakdown calculates _source_concept_id from source fields in CDM tables.
4. Vocabulary database schema is now supported.
5. Metadata (vocabulary version information from data source) is now collected.
6. OracleTempSchema use deprecated in favor of tempEmulationSchema.
7. Run against external concept count has been removed, as concept counts data is not available. Function 'runCohortDiagnosticsUsingExternalCounts' is removed.
8. Removed code related to referentConceptId = phenotypeId/1000 as it does not always hold true.
9. Create cohort table function is now private. Please use instantiate cohort.
10. checkInputFileEncoding is not exported as a public function (as not the scope of CohortDiagnostics).
11. Updated results data model to include new tables (resolved_concepts).
12. Cohort Diagnostics results data model now compliant with standard characterization output.
13. Support for cohort_censor_stats table in webapi 2.8.1 #387
14. Add time series diagnostics computation. Output is not in Diagnostics explorer in this version.
15. Any improvements to help with usability and stability. Informative messages to help with debugbing as needed.
16. phenotypeDescription is no longer supported as input for cohort diagnostics.
Bug fixes:

1. databaseName and databaseDescription should be non NULL
2. Fixed computation of standard deviation and standard difference of mean for binary covariates.


CohortDiagnostics 2.0.0
=======================

Changes:

1. Many improvements in performance when computing diagnostics. Now allows computation of diagnostics for many (>100) cohorts at once. 

2. The Diagnostics Explorer Shiny app can now also run against results data in a database. Added functions for uploading the diagnostics data to a database. This becomes necessary when dealing with very large data (e.g. more than 100 cohort definitions).

3. Added ability to group cohort definitions by phenotypes, and add descriptions of phenotypes. Currently the only consequence of grouping by phenotype is that cohort overlap is only computed within a phenotype. Grouping by phenotypes is done by adding a `phenotypeId` field to the `cohortsToCreate` file, and by adding a `phenotypeDescription` file. See the `examplePhenotypeLibraryPackage` in the repo for an example.

4. The cohort overlap plot now uses stacked bar charts instead of a Venn diagram to allow showing multiple comparisons across multiple databases at once.

5. The cohort characteristics comparison plot now colors by domain, and allows filtering by domain.

6. Switching from 'plotly' to 'ggiraph' for interactive plotting.

7. Added a Visit Context diagnostic.


Bug fixes:

1. Fixing numerous bugs introduced in v1.2.x.


CohortDiagnostics 1.2.6
=======================

Bug fixes:

1. Additional bug fixes for characterization/temporal characterization.


CohortDiagnostics 1.2.5
=======================

Bug fixes:

1. Additional bug fixes for characterization/temporal characterization.


CohortDiagnostics 1.2.4
=======================

Bug fixes:

1. Added details log when characterization/temporal characterization does not return results or returns result below threshold value. By default we filter out results in from Characterization and Temporal Characterization where the value is less than 0.001. This was leading to empty results for some cohorts - causing errors. 


CohortDiagnostics 1.2.3
=======================

Bug fixes:

1. Fixed error when many concept sets have to be instantiated.
2. Removed ohdsi/SqlRender from Remotes https://github.com/OHDSI/CohortDiagnostics/issues/189
3. Fixed Digit precision for RJSONIO::toJson and fromJSON https://github.com/OHDSI/CohortDiagnostics/issues/161 This is an important fix. If digit precision is not explicitly specified in RJSONIO, then scientific notation is used. This issue seems to only happen when an integer id (conceptId, conceptSetId, cohortId etc) >= 10,000,000 (which is rare). Please use this update if you have id's > 10,000,000.


CohortDiagnostics 1.2.2
=======================
New features:
1. Minor UI changes to Diagnostics explorer. Added missing sort.
2. Added better labels for plots.
3. Download plots.

Bug fixes:
1. Changes dependency to ROhdsiWebApi (>= 1.1.0)
2. DiagnosticsExplorer display bug fixes

CohortDiagnostics 1.2.1
=======================
New features:
1. All objects in DiagnosticsExplorer are sorted by default #173
2. Multi select for concepts #199

Bug fixes:
1. Ensure concept sets across cohort definitions are unique #174 (changes dependency to ROhdsiWebApi (>= 1.1.0))

CohortDiagnostics 1.2.0
=======================
Changes:
1. New function to retrieve concept set json from cohort json \code{extractConceptSetsJsonFromCohortJson}
2. New function to retrieve concept set sql from cohort sql \code{extractConceptSetsSqlFromCohortSql}
3. DiagnosticsExplorer shiny app - DataTable now rendered using server side processing. Bug fixes and UI improvements.
4. DiagnosticsExplorer shiny app - Phenotype library mode (released)
5. DiagnosticsExplorer shiny app - Combine included source concepts and orphan concepts into one submenu https://github.com/OHDSI/CohortDiagnostics/issues/129

Bug fixes:
1. https://github.com/OHDSI/CohortDiagnostics/issues/167
2. https://github.com/OHDSI/CohortDiagnostics/issues/165


CohortDiagnostics 1.1.1
=======================
Changes:
1. Shiny app UI improvements
2. Link out to Atlas and Athena from cohortId. Supports baseUrl.
3. (beta - unreleased) support for Phenotype library. Shiny app will look for two additional csv files phenotypeDescription and cohortDescription that put the DiagnosticExplorer in Phenotype Library Mode. Plan to release in future version >= 1.2
4. Changed default selections for temporal characterization
5. Added minimum threshold value to covariate_value and temporal_covariate_value with default value = 0 (future release, we plan to make this 0.005 i.e. 0.5%)

Bug fixes:
1. Minor bug fixes.

CohortDiagnostics 1.1.0
=======================

Changes: 
1. Added temporal characterization
2. UI changes to Shiny app diagnostic explorer

Bug fixes:
1. Circe-be update introduced bug in parsing concept sets in cohort definition. 
2. Handling of empty cohorts

Changes:

1. Error handling: Use [checkmate](https://CRAN.R-project.org/package=checkmate) R-package to provide more informative error messages.
2. Refactor runCohortDiagnostics: added new function by refactoring existing private functions. This new function
get the JSON and parameterized OHDSI SQL for the cohorts for which diagnostics has been requested \code{getCohortsJsonAndSql} 

Note: The code has been partially refactored to depend on tidyverse. 

CohortDiagnostics 1.0.2
=======================

Changes:

1. Adapting to new ROhdsiWebApi (>= 1.0.0) interface.

2. Added log info that provides addition run time details #72

Bugfixes:

1. Fixing broken getTimeDistributions function.

2. Fixing broken instantiateCohort function.


CohortDiagnostics 1.0.0
=======================

Changes:

1. Added database information tab to Diagnostics Explorer Shiny app.

2. Using Andromeda instead of ff to store large data objects (used for characterization).

Bugfixes: 

1. Fixed error when cohort definition has no concept sets.

2. Fixed error in index event breakdown when entry event contained multiple criteria for the same domain.

3. Fixed error related to tempEmulationSchema argument (needed on Oracle and BigQuery).

4. Fixed use of deprecated function, causing warnings and causing older versions of ParallelLogger to error.


CohortDiagnostics 0.1.2
=======================

Changes

1. Adding option to run cohort instantiation and diagnostics incrementally, so only running those tasks that have changed since the last ru.


CohortDiagnostics 0.1.1
=======================

Changes

1. Orphan and included concepts tables now show concept ID.

Bugfixes: 

1. Fixed error in orphan code SQL (hard-coded database schema).

2. Fixed bug when concept counts become too big for a regular INT.

3. Fixed counts of standard concepts shown when using external concept count table.


CohortDiagnostics 0.1.0
=======================

Changes

1. Orphan code check now resolves concept sets instead of using verbatim concepts. Should include specificity.

2. Added option to use the same y-axis for all databases in incidence rate plot.

3. Added runCohortDiagnosticsUsingExternalCounts function.

4. Added option to use cohort definitions in WebAPI instead of those stored in study package.


Bugfixes:

1. Fixed bug causing weird false positives in orphan codes (caused by transforming concept IDs to scientific notation).

2. Better handling in viewer when some analyses are not executed.

3. Now also showing cohorts that had zero entries.

4. Fixed error when there was only 1 cohort.

CohortDiagnostics 0.0.1
=======================

Initial version
