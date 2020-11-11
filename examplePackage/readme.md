Example Package
===============

This study package demonstrates how to currently use CohortDiagnostics in a package. The package only contains code to run StudyDiagnostics.

To modify the package to include the cohorts of interest, take these steps:

1. Copy/download the *examplePackage* folder. For example, download the Zip file [here](https://github.com/OHDSI/CohortDiagnostics/archive/master.zip), open it, and locate the *examplePackage* folder and extract it.

2. Change the package name as needed. Most importantly:
    - Change the `Package:` field in the *DESCRIPTION* file, 
    - The `packageName` argument in the *R/CohortDiagnostics* file, and
    - The `library()` call at the top of *extras/CodeToRun.R*
    - The name of the *.Rproj* file.
    
3. Open the R project in R studio (e.g. by double-clicking on the *.Rproj* file).

4. Modify *inst/settings/CohortsToCreate.csv* to include only those cohorts you are interested in. Fill in each of the four columns:

    - **phenotypeId**: This is an unique integer ID for a phenotype in the [OHDSI Phenotype Library](https://data.ohdsi.org/PhenotypeLibrary/). If you don't find an appropriate phenotypeId to capture your clinical idea, then you may use phenotype = 0. Advanced users may consider using custom phenotype ids.
    - **cohortId**: This is an unique integer ID for a cohort definition that is in the [OHDSI Phenotype Library Cohort Diagnostics.csv](https://github.com/OHDSI/PhenotypeLibrary/blob/master/extras/CohortDescription.csv). If you need to use a cohort definition that is not currently in the OHDSI Phenotype library (this commonly happens when you are developing a new cohort definition), you may leave this field maybe the webApiCohortId.
    - **webApiCohortId**: The unique integer id of your cohort in your Atlas/WebApi instance. 
    - **cohortName**: The unique string name for your cohort.
    - **logicDescription**: A concise (not more than two short sentences) description of the logic behind your cohort definition. The purpose of this field is to help you, or someone else, easily identify this cohort definition and contrast it to other similar cohort definitions. This is required if you are attempting to build and diagnose a new cohort definition that is not currently in the OHDSI Phenotype Library, and not required if you are diagnosing a cohort definition that is already in the OHDSI phenotype library.


5. Run this code (note, this can also be found in *extras/PackageMaintenance.R*):

    ```r
    # If ROhdsiWebApi is not yet installed:
    install.packages("devtools")
    devtools::install_github("ROhdsiWebApi")
    
    baseUrl = "" # please provide the base url for your webApi
    
    CohortsToCreate %>% 
    dplyr::select(.data$webApiCohortId) %>% 
    dplyr::rename(atlasId = .data$webApiCohortId) %>% 
    dplyr::mutate(cohortId = .data$atlasId, name = as.character(.data$atlasId)) %>% 
    readr::write_excel_csv(file = file.path(tempdir, 'CohortsToCreate.csv'), na = "")
    
    ROhdsiWebApi::insertCohortDefinitionSetInPackage(fileName = file.path(tempdir, 'CohortsToCreate.csv'),
                                                     baseUrl = <baseUrl>,
                                                     insertTableSql = TRUE,
                                                     insertCohortCreationR = TRUE,
                                                     generateStats = TRUE,
                                                     packageName = <package name>)
    ```
    
    Where `<baseUrl>` is the base URL for the WebApi instance, for example: "http://server.org:80/WebAPI", and `<package name>` is the name of your new package.

You can now build your package. See *extras/CodeToRun.R* on how to run the package.

