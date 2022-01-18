CohortDiagnostics
=================

[![Build Status](https://github.com/OHDSI/CohortDiagnostics/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CohortDiagnostics/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/CohortDiagnostics/coverage.svg?branch=master)](https://codecov.io/github/OHDSI/CohortDiagnostics?branch=master)

CohortDiagnostics is part of [HADES](https://ohdsi.github.io/Hades).

Introduction
============
This is an R package for performing various study diagnostics, many of which are not specific to any particular study design.

Features
========
- Show cohort inclusion rule attrition. 
- List all source codes used when running a cohort definition on a specific database.
- Find orphan codes, (source) codes that should be, but are not included in a particular concept set.
- Compute cohort incidence across calendar years, age, and gender.
- Break down index events into the specific concepts that triggered them.
- Compute overlap between two cohorts.
- Characterize cohorts, and compare these characterizations. Perform cohort comparison and temporal comparisons. 
- Explore patient profiles of a random sample of subjects in a cohort.

Screenshot
==========
![The Diagnostics Explorer Shiny app](vignettes/shiny.png)

Technology
==========
The CohortDiagnostics package is an R package.

System Requirements
===================
Requires R. Some of the packages used by CohortDiagnostics require Java.

Installation
=============

1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including Java.

2. In R, use the following commands to download and install CohortDiagnostics:

  ```r
  remotes::install_github("OHDSI/CohortDiagnostics")
```

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/CohortDiagnostics).

PDF versions of the documentation are also available:

* Package manual: [CohortDiagnostics manual](https://raw.githubusercontent.com/OHDSI/CohortDiagnostics/master/extras/CohortDiagnostics.pdf) 
* Vignette: [What is Cohort Diagnostics](https://raw.githubusercontent.com/OHDSI/CohortDiagnostics/master/inst/doc/WhatIsCohortDiagnostics.pdf)
* Vignette: [Running Cohort Diagnostics](https://raw.githubusercontent.com/OHDSI/CohortDiagnostics/master/inst/doc/RunningCohortDiagnostics.pdf)
* Vignette: [Viewing Results Using Diagnostics Explorer](https://raw.githubusercontent.com/OHDSI/CohortDiagnostics/master/inst/doc/ViewingResultsUsingDiagnosticsExplorer.pdf)


Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/CohortDiagnostics/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
CohortDiagnostics is licensed under Apache License 2.0

Development
===========
CohortDiagnostics is being developed in R Studio.

### Development status

Stable
