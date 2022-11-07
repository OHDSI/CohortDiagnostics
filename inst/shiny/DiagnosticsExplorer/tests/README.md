# Testing diagnostics explorer

To run tests on the shiny apps use `testthat::test_dir` as follows.
From the context of a working directory at the package root:

``` {r}
testthat::test_dir("inst/shiny/DiagnosticsExplorer/tests")
```

From the context of a standalone DiagnosticsExplorer instance

``` {r}
testthat::test_dir("tests")
```