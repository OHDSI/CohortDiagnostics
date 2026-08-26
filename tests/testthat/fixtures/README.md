# Test Fixtures

This directory contains test fixtures and mock data generators for CohortDiagnostics unit tests.

## Philosophy

Fixtures provide reusable test data that:
- Is fast to create (no database required)
- Is deterministic (same input = same output)
- Is minimal (only what's needed for the test)
- Is isolated (no shared state between tests)

## Available Fixtures

### `mock_data.R`
Functions to generate mock data structures:
- `createMockCohortDefinitionSet()` - Mock cohort definitions
- `createMockConnectionDetails()` - Mock database connection details
- `createMockCohortCounts()` - Mock cohort count data
- `createMockTemporalCovariateData()` - Mock temporal covariate data
- `createMockIncidenceRateData()` - Mock incidence rate data
- `createMockTimeSeriesData()` - Mock time series data

### `test_cohorts.R`
Pre-defined minimal cohort definitions for testing:
- `getMinimalCohortDefinitionSet()` - Single minimal cohort
- `getSingleCohortDefinition()` - One complete cohort definition
- `getMultipleCohortDefinitions()` - Multiple cohorts for testing relationships

### `sqlite_helpers.R`
SQLite-specific helpers for tests that need a real database:
- `createInMemorySqliteDb()` - Create in-memory SQLite database
- `populateMinimalCdmSchema()` - Add minimal CDM tables
- `createMockCohortTable()` - Create and populate cohort table

## Usage Example

```r
test_that("Function processes cohort definitions correctly", {
  # Arrange
  cohortDefs <- createMockCohortDefinitionSet(numCohorts = 3)
  
  # Act
  result <- myFunction(cohortDefs)
  
  # Assert
  expect_equal(nrow(result), 3)
})
```

## Best Practices

1. **Use the simplest fixture possible** - Don't create a full database if you just need a data frame
2. **Create fixtures in the test** - For simple data, create it inline rather than using a fixture function
3. **Use fixtures for complex data** - When setup is complex or reused across tests
4. **Don't modify fixtures** - Treat them as immutable; copy if you need to modify
5. **Clean up after yourself** - Use `withr::defer()` or `on.exit()` for cleanup
