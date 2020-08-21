CohortDiagnostics (Develop)
=======================

Note: 1.2.x are the last planned release in v1.x.x series. No new functionalities will 
be added to 1.2.x moving forward. All future releases in 1.2.x series will be bug fixes.

Version 2.x is planned for release by October 2020, and will be a breaking change. Output of 
Versions 2.x will use a different data model and will not be backward compatible with 1.x.

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
New features:
1. All objects in DiagnosticsExplorer are sorted by default #173
2. Multi select for concepts #199

Bug fixes:
1. Ensure concept sets across cohort definitions are unique #174

CohortDiagnostics 1.2.0
=======================
New features:
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
New features:
1. Shiny app UI improvements
2. Link out to Atlas and Athena from cohortId. Supports baseUrl.
3. (beta - unreleased) support for Phenotype library. Shiny app will look for two additional csv files phenotypeDescription and cohortDescription that put the DiagnosticExplorer in Phenotype Library Mode. Plan to release in future version >= 1.2
4. Changed default selections for temporal characterization
5. Added minimum threshold value to covariate_value and temporal_covariate_value with default value = 0 (future release, we plan to make this 0.005 i.e. 0.5%)

Bug fixes:
1. Minor bug fixes.

CohortDiagnostics 1.1.0
=======================

New features: 
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

CohortDiagnostics 1.0.0
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

3. Fixed error related to oracleTempSchema argument (needed on Oracle and BigQuery).

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