CohortDiagnostics (Develop)
=======================

Changes:

1. Error handling: Use [checkmate](https://CRAN.R-project.org/package=checkmate) R-package to provide more informative error messages.
2. Refactor runCohortDiagnostics: added new function by refactoring exisitng private functions. This new function
get the JSON and parameterized OHDSI SQL for the cohorts for which diagnostics has been requested \code{getCohortsJsonAndSql} 

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