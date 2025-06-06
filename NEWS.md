CohortDiagnostics 3.4.1
=======================

Bug fixes:

1. Fixed issue loading concept sets in index event breakdown - caused by change to jsonlite parser.

CohortDiagnostics 3.4.0
=======================

Bug Fixes:

1. Exporting concept information now uses the vocabulary database schema, which may be different from the cdm schema


CohortDiagnostics 3.3.1
=======================
Changes:

1. Added support for different measurements from FeatureExtraction - measurement as value, measurement value as concept id
and measurement range groups (above range, in normal range, below normal)

Bug fixes:

1. Attempted fix of issue with DATEDIFF overflowing integer on some databse platforms


CohortDiagnostics 3.3.0
=======================

Changes:

1. Added function to make deployment to posit connect servers easier

2. Added ability to use CohortGenerator sample functionality to executeDiagnostics which speeds up execution for very
large cohort definitions

3. Requires use of FeatureExtraction 3.4.0 to support new API

4. Removed annotation tables from result schema script

CohortDiagnostics 3.2.5
=======================
Bug Fixes:

1. fixed broken migrations using "ADD COLUMN "

2. Fixed broken links in manaual

3. Fixed issue with Andromeda calls in characterization on R 4.3.x

4. Fix for Vroom issue on mac

CohortDiagnostics 3.2.4
=======================
Bug Fix:

1. Added support for newer API to OhdsiShinyModules (while maintaining support for old version)

2. Resolved issue with incidence rates sometimes exporting as null/infinite by setting to 0 in all error cases


CohortDiagnostics 3.2.3
=======================

1. Resolved issue where concept sets were only exported when diagnostics for them were executed. All concept sets are
now always exported into results csv and imported into databases

CohortDiagnostics 3.2.2
=======================

1. Fixes to unit tests breaking with R 4.3 due to change of Sys.time function


CohortDiagnostics 3.2.1
=======================

1. Added requirement for Andromeda 0.6.0 (which is implicitly required by DatabaseConnector)

2. Fixed issue saving cohort subsets to csv file for viewing and analysis in shiny app

CohortDiagnostics 3.2.0
=======================

1. Do not run orphan concepts checks for any subset cohorts

2. Remove use of lookback period for IR calculations - this is now a setting of the call to the package

3. Added data migration to support subsets in database schema (allow future functionality to take care of them)

4. Added functionality to `launchDiagnosticsExplorer` to make publishing to poist connect/shinyapps.io more straightforward (still requires removal of ggiraph)

5. Moved most shiny code to `OHDSI/OhdsiShinyModules`


CohortDiagnostics 3.1.2
=======================
Bug Fixes:

1. Removed package dependency snapshot capture as it was breaking on newer versions of R


CohortDiagnostics 3.1.1
=======================
Changes:

1. Removed CohortExplorer app as it's now part of a [new package](https://github.com/ohdsi/cohortExplorer).

2. Added support for custom FeatureExtraction features

Bug Fixes:

1. Fixed error when checking for cdm_source table

2. Removal of `.data$` usage across package to fix tidyselect warning

CohortDiagnostics 3.1.0
=======================
Changes:

1. Major refactoring of shiny app to use modular code for ease of maintenance

2. Some tests for shiny modules in `inst/shiny/DiagnosticsExplorer/tests`

3. Added support for table prefixes in Diagnostics Explorer databases (e.g. cg_cohort_definition)

4. Enabled annotation on local instances of DiagnosticsExplorer

5. Added yaml configuration for diagnostics explorer app, including docs on usage

6. Storage of version number in database results file to allow future migrations

7. Slight optimization of cohort characterization queries in diagnostics explorer

8. Support for database migrations by adding `migrateDataModel` functionality.
Versions of data generated with CohortDiagnostics 3.0.0 are intended to be future compatible.
i.e. if you have an sqlite results file or postgres database generated with version 3.0.0 of cohort diagnostics
new shiny app functionality will be supported if you run ``migrateDataModel`` on it.

9. Changed type to dataType and fieldName to columnName to align with strategus modules

9. Refactored export of characterization results to use common export

10. Ensure that tests cases always use a continuous covariate

11. Batch operations for executing cohort relationship, time series, and feature extraction based diagnostics.

13. New parameter minCharacterizationMean. This introduces a cut off for the output of FeatureExtraction. In the absence of the parameter the output would have atleast one row for every covariateId in the datasource  - most having very low count to be useful for diagnostics.

Bug fixes:

1. Fixed issue uploading results to postgres db caused by null values in primary key field. 
Removed constraint to fix issue.

2. Fix for `index_event_breakdown` having duplicate entries where concept is observed in the same domain multiple times

3. Many other issues resolved in shiny codebase


CohortDiagnostics 3.0.3
=======================

Changes:

1. Changed default batch size for characterization feature extraction from 100 to 5 as it was causing performance issues
on redshift clusters.

2. Allow setting of batch size for feature extraction with `options("CohortDiagnostics-FE-batch-size" = batchSize)`

CohortDiagnostics 3.0.2
=======================

Bug fixes:

1. Fixed issue with writing csvs caused by update to SqlRender camelCaseToSnakeCase function check that caused execution
to crash if parameters were null.

3. Fixed issue with observation period overflowing sql integer on BigQuery causing execution to crash

CohortDiagnostics 3.0.1
=======================

Bug fixes:

1. Updated old/incorrect documentation on package usage

2. Fixed bug with new versions of CohortGenerator v0.5.0 causing cohort definition sets in package to not load

3. Fixed bug in shiny app where multiple runs on the same database would cause the app to crash when selecting database

CohortDiagnostics 3.0.0
=======================
Changes:

1. Time series diagnostics removed

2. Removed runCohortDiagnostics function - this has now been completely replaced with executeDiagnostics

3. Removed `loadCohortsFromPackage` function as this is now replaced with
`CohortGenerator::getCohortDefinitionSet`

4. Removed instantiate cohort functionality, `instantiateCohortSet` should now be used with the `CohortGenerator` package

5. Removed optional `inclusionStatisticsFolder` parameter, this is now all exported directly from `CohortGenerator` 
without the need to generate this first.

6. Removed usage of Rdata files in DiagnosticsExplorer shiny app and function to create them `preMergeDiagnosticsFiles`

7. Added function `createMergedResultsFile` which outputs a shiny app

8. Added support for any `SqlRender/DatabaseConnector` compatible database (note, 
this is experimental. Postgres and sqlite are the only backends recommended for use in production environments)

9. Improved metadata collection and storage from runs of cohort diagnostics.

10. Removed phenotype_id field from data ddl

11. Additional checks to the output of cohort diagnostics to ensure it conforms to its own results data model. The new function (internal) is makeDataExportable. Results data model csv file has been enhanced with new fields, including a field to specify if the value is to be subjected to privacy protection (i.e. min cell count, eg. person count). Note a bug was discovered in the orphan concepts and included source concepts that was leading to duplication of row records by primary key. This bug has been fixed by calculating its max value grouped by primary keys. It will be fixed in another commit.

12. New optional diagnostics computes temporal relationship between any two cohorts. The settings for the temporal relationship between cohorts defaults to be the same as temporalCovariateSettings. This diagnostics will be integrated into the characterization output of diagnostics explorer, where cohorts will be covariates.

13. New optional diagnostics called time series diagnostics. Time series diagnostics takes as input a calendar period range, and in that calendar period range for calendar units (year, quarter, month) computes the approximate new occurrence (approximates incidence) and observations (approximates prevalence) of the cohort start and cohort end dates during the calendar period. 

Bug fixes:

1. Added support for users to include non-standard columns in their CDM preventing crashes


CohortDiagnostics 2.2.4
=======================

Bug fixes:

1. Fixed a bug that was causing generation of premerged and upload files to fail when the output had fields that were not in the results data model.


CohortDiagnostics 2.2.3
=======================

Bug fixes:

1. Replace use dplyr across() for bug introduced by tidyr v1.2.0


CohortDiagnostics 2.2.2
=======================

Bug fixes:

1. Fixed syntax error causing empty description field in inclusion rule stats


CohortDiagnostics 2.2.1
=======================

Bug fixes:

1. Added new lines in NEWS.md to fix package site.

2. Updated vignette to fix minor issues

3. Cohorts with zero counts are now stored in results and display in shiny app

CohortDiagnostics 2.2.0
=======================
Changes:
1. Added `executeDiagnostics` function which aims to replace `runCohortDiagnostics`as the main interface to the package.

2. Updated vignette on "Running Cohort Diagnostics" to give clearer instructions

3. Removed vignettes on usage that are no longer required

4. Improved testing across database platforms

5. Moved some not particularly useful warnings to `logInfo`

Bug Fixes:
1. User code removed from CohortExplorer to fix issue #618

2. Fixed bug with `runBreakdownIndexEvents = TRUE` failed for drug_era table with SQL error - Issue #695

3. Fixed error when computing incidence rates on BigQuery.

4. Fixed error when `cdm_source` table is empty (warning remains).

5. Fixed error when instantiating cohorts on BigQuery.

CohortDiagnostics 2.1.4
=======================

Changes:

1. Minor cosmetic changes to diagnostics explorer shiny app. Typo fix

2. Fix for warning from type-convert https://github.com/OHDSI/CohortDiagnostics/issues/661

3. Use Sex instead of Gender in Diagnostics Explorer https://github.com/OHDSI/CohortDiagnostics/issues/676

Bug Fixes:

1. Privacy protecting feature bug fix. In prior version covariate_value and covariate_value_dist failed privacy protection. Thank you @msuchard for reporting the issue and @schuemie for fixing https://github.com/OHDSI/CohortDiagnostics/issues/658 

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

18. databaseName and databaseDescription should be non NULL

19. Fixed computation of standard deviation and standard difference of mean for binary covariates.


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
