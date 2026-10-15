# OHDSI Analysis Viewer

## Table of contents
1. [Introduction](#intro)
2. [How to use the viewer](#use)
3. [Analysis types](#types)
    1. [Characterization](#char)
    2. [Population-level effect estimation](#ple)
    3. [Patient-level prediction](#plp)

## Introduction <a name="intro"></a>

This is an interactive shiny app for exploring standardized outputs for OHDSI analyses including:

- characterization (descriptive studies)
- population-level effect estimation(causal inference)
- patient-level prediction (inference)

Full details of all the analysis tools can be found on the [HADES website](https://ohdsi.github.io/Hades)



## How to use the viewer <a name="use"></a>

Please use the left hand menu to select the type of analysis to explore (click on a button).  This show the results that can be interactively explored.

## Analysis types <a name="types"></a>

### Characterization <a name="char"></a>

The OHDSI community have developed a suite of tools for conducting characterization studies including:

- incidence rate calculation
- baseline characterization 
- treatment pathways
- and more

### Population-level effect estimation <a name="ple"></a>

The OHDSI community have developed several packages that enable users with data in the OMOP common data model to perform causal inference studies.

- [CohortMethod](https://ohdsi.github.io/CohortMethod)
- [SelfControlledCaseSeries](https://ohdsi.github.io/SelfControlledCaseSeries)
- [SelfControlledCohort](https://ohdsi.github.io/SelfControlledCohort)

### Patient-level prediction <a name="plp"></a>

The OHDSI community have developed several packages that enable users with data in the OMOP common data model to develop and validate patient-level prediction models.

- [PatientLevelPrediction](https://ohdsi.github.io/PatientLevelPrediction)
- [EnsemblePatientLevelPrediction](https://ohdsi.github.io/EnsemblePatientLevelPrediction)
- [DeepPatientLevelPrediction](https://ohdsi.github.io/DeepPatientLevelPrediction)



