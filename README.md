CohortDiagnostics
================

[![Build Status](https://travis-ci.org/OHDSI/CohortDiagnostics.svg?branch=master)](https://travis-ci.org/OHDSI/CohortDiagnostics)
[![codecov.io](https://codecov.io/github/OHDSI/CohortDiagnostics/coverage.svg?branch=master)](https://codecov.io/github/OHDSI/CohortDiagnostics?branch=master)

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
- Characterize cohorts, and compare these characterizations.
- Explore patient profiles of a random sample of subjects in a cohort.

Screenshot
==========
![The Diagnostics Explorer Shiny app](vignettes/shiny.png)

Technology
==========
The CohortDiagnostics package is an R package.

System Requirements
===================
Running the package requires R with the package rJava installed. Also requires Java 1.7 or higher.

Installation
=============
## R package

To install the latest development version, install from GitHub:

```r
install.packages("devtools")
devtools::install_github("ohdsi/ROhdsiWebApi")
devtools::install_github("ohdsi/FeatureExtraction")
devtools::install_github("ohdsi/CohortDiagnostics")
```

User Documentation
==================
* Vignette: [Running cohort diagnostics using WebAPI](https://raw.githubusercontent.com/OHDSI/CohortDiagnostics/master/inst/doc/CohortDiagnosticsUsingWebApi.pdf)
* Package manual: [CohortDiagnostics manual](https://ohdsi.github.io/CohortDiagnostics/reference/index.html) 

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/CohortDiagnostics/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

License
=======
CohortDiagnostics is licensed under Apache License 2.0

Development
===========
CohortDiagnostics is being developed in R Studio.

### Development status

Beta testing
