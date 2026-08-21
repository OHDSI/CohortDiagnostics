# CohortDiagnostics Testing Guide

## Overview

The CohortDiagnostics test suite is organized into two types of tests:

1. **Unit Tests** - Fast, isolated tests that don't require database connections
2. **Integration Tests** - Slower, end-to-end tests that require database setup

## Test Structure

```
tests/
├── testthat.R            # Standard testthat entrypoint (used by R CMD check / CRAN)
└── testthat/
├── fixtures/              # Test data generators and fixtures
│   ├── README.md
│   ├── mock_data.R       # Mock data generators
│   ├── test_cohorts.R    # Pre-defined test cohorts
│   └── sqlite_helpers.R  # SQLite test database helpers
├── mocks/                # Mock implementations
│   ├── database_mocks.R
│   └── feature_extraction_mocks.R
├── unit/                 # Fast unit tests (no database)
│   ├── test-unit-template.R
│   ├── test-unit-incremental.R
│   └── ... (more unit tests)
├── integration/          # Slow integration tests (require database)
│   ├── helpers-integration.R
│   ├── test-integration-full.R
│   ├── test-integration-results-dm.R
│   └── ... (more integration tests)
├── setup.R              # Conditional setup (integration only)
├── helper.R             # Generic helper functions
└── test-5-incremental.R # Remaining tests (to be migrated)
```

## Running Tests

### Unit Tests Only (Fast - Recommended for Development)

```r
# From R console
library(testthat)
test_dir("tests/testthat/unit")
```

```bash
# From command line
Rscript tests/testSqlite.R
```

Unit tests:
- Run in < 10 seconds
- No database connections required
- Use mocks and fixtures
- Run on every commit in CI/CD

### Integration Tests (Slow - Run Before Merge)

```r
# From R console
Sys.setenv(INTEGRATION_TESTS = "TRUE")
library(testthat)
test_dir("tests/testthat/integration")
```

```bash
# From command line
INTEGRATION_TESTS=TRUE Rscript tests/testSqlite.R
```

Integration tests:
- Run in several minutes
- Require database setup (Eunomia for SQLite)
- Test end-to-end functionality
- Run on PR merge or nightly
- Are always skipped on CRAN via `testthat::skip_on_cran()`
  in `test-integration-runner.R`

### All Tests

```bash
# Run both unit and integration tests
INTEGRATION_TESTS=TRUE RUN_ALL_TESTS=TRUE Rscript tests/testSqlite.R
```

### R CMD check / CRAN

`tests/testthat.R` is the standard testthat entrypoint used by `R CMD check`:

```r
library(testthat)
library(CohortDiagnostics)
test_check("CohortDiagnostics")
```

On CRAN this runs unit tests and skips integration tests (via
`skip_on_cran()`). Integration tests only run when `INTEGRATION_TESTS=TRUE`
and `NOT_CRAN=true` are set.

## Writing Tests

### Unit Test Guidelines

1. **Keep tests isolated** - No shared state between tests
2. **Use fixtures** - See `fixtures/mock_data.R` for data generators
3. **Mock external dependencies** - Database, FeatureExtraction, etc.
4. **Follow AAA pattern** - Arrange, Act, Assert
5. **Test edge cases** - Empty inputs, NULL values, errors
6. **Keep tests fast** - Each test should run in < 1 second

### Unit Test Example

```r
test_that("function processes data correctly", {
  # Arrange
  mockData <- createMockCohortDefinitionSet(numCohorts = 3)
  
  # Act
  result <- myFunction(mockData)
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_true(all(c("cohortId", "cohortName") %in% names(result)))
})
```

### Integration Test Guidelines

1. **Test end-to-end workflows** - Full diagnostic execution
2. **Use real databases** - SQLite (Eunomia) or other platforms
3. **Test database interactions** - SQL generation, data upload
4. **Verify file outputs** - CSV exports, SQLite databases
5. **Clean up after tests** - Use `withr::defer()` or `on.exit()`

### Integration Test Example

```r
test_that("executeDiagnostics runs successfully", {
  skip_if(!isIntegrationTestMode(), "Integration tests not enabled")
  
  # Arrange
  # (setup.R has already created connection and cohorts)
  
  # Act
  executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    exportFolder = folder,
    databaseId = "test"
  )
  
  # Assert
  expect_true(file.exists(file.path(folder, "cohort_count.csv")))
})
```

## Available Fixtures

### Mock Data Generators (`fixtures/mock_data.R`)

- `createMockCohortDefinitionSet(numCohorts)` - Mock cohort definitions
- `createMockCohortCounts(cohortIds)` - Mock cohort counts
- `createMockTemporalCovariateData(cohortIds)` - Mock temporal covariates
- `createMockIncidenceRateData(cohortId)` - Mock incidence rates
- `createMockTimeSeriesData(cohortIds)` - Mock time series data

### Test Cohorts (`fixtures/test_cohorts.R`)

- `getMinimalCohortDefinitionSet()` - Single minimal cohort
- `getSingleCohortDefinition(cohortId)` - One complete cohort
- `getMultipleCohortDefinitions(numCohorts)` - Multiple cohorts
- `getCohortDefinitionWithSubset()` - Parent and subset cohorts
- `getCohortDefinitionWithConceptSets(numConceptSets)` - Cohort with concept sets

### SQLite Helpers (`fixtures/sqlite_helpers.R`)

- `createInMemorySqliteDb()` - In-memory SQLite connection
- `populateMinimalCdmSchema(connection)` - Add minimal CDM tables
- `createMockCohortTable(connection)` - Create cohort table with data
- `setupTestDatabase()` - Complete test database setup

## Mocking

### Database Mocks (`mocks/database_mocks.R`)

```r
# Mock connection
conn <- mockDatabaseConnection(dbms = "sqlite")

# Mock query result
result <- mockQueryResult(data.frame(id = 1:3))
```

### FeatureExtraction Mocks (`mocks/feature_extraction_mocks.R`)

```r
# Mock covariate data
covData <- mockGetDbCovariateData(
  connection = conn,
  cohortId = 1,
  covariateSettings = settings
)
```

## Test Organization

### Current Status

- ✅ **Unit Tests**: Incremental mode functions
- ✅ **Integration Tests**: Moved to `integration/` directory
- 🔄 **In Progress**: Additional unit tests for other modules
- ⏳ **Planned**: Complete unit test coverage (Phase 2)

### Migration Plan

Existing tests in root `testthat/` directory will be:
1. Analyzed for unit test opportunities
2. Split into unit and integration components
3. Moved to appropriate directories
4. Enhanced with better isolation and mocking

## CI/CD Integration

### GitHub Actions Workflow

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run unit tests
        run: Rscript tests/testSqlite.R
        
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run integration tests
        run: INTEGRATION_TESTS=TRUE Rscript tests/testSqlite.R
```

## Troubleshooting

### Unit Tests Failing

1. Check that fixtures are being sourced correctly
2. Verify no database connections are being made
3. Ensure tests are truly isolated (no shared state)
4. Check for missing mock implementations

### Integration Tests Failing

1. Verify `INTEGRATION_TESTS=TRUE` is set
2. Check database connection details
3. Ensure JDBC drivers are available
4. Verify cohorts were generated successfully
5. Check for cleanup issues (temp files, tables)

### Tests Running Slowly

1. Move slow tests to `integration/` directory
2. Add mocks for database interactions
3. Use smaller test datasets
4. Check for unnecessary file I/O

## Best Practices

### DO

- ✅ Write unit tests for all new functions
- ✅ Use descriptive test names
- ✅ Test edge cases and error conditions
- ✅ Clean up after tests (temp files, connections)
- ✅ Use fixtures for complex test data
- ✅ Keep tests independent and isolated

### DON'T

- ❌ Share state between tests
- ❌ Require database for unit tests
- ❌ Write tests that depend on execution order
- ❌ Leave temp files or connections open
- ❌ Test implementation details (test behavior)
- ❌ Write tests that take > 1 second (unit tests)

## Getting Help

- See `unit/test-unit-template.R` for examples
- Check `fixtures/README.md` for fixture documentation
- Review existing unit tests for patterns
- Ask in team chat or create an issue

## Future Enhancements

Phase 2 will add comprehensive unit tests for:
- Cohort characterization
- Incidence rates
- Time series
- Concept sets
- Visit context
- Results data model
- And more...

See `test_refactoring_plan.md` for details.
