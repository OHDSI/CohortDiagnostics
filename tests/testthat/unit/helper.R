# Source parent-level helpers and fixtures for unit tests
# test_dir("unit") looks for helper*.R in unit/ only, not parent directory

suppressMessages({
  parent_helpers <- list.files(
    testthat::test_path(".."),
    pattern = "^helper.*\\.R$",
    full.names = TRUE
  )
  for (f in parent_helpers) {
    source(f)
  }

  fixture_dir <- testthat::test_path("..", "fixtures")
  if (dir.exists(fixture_dir)) {
    fixture_files <- list.files(fixture_dir, pattern = "\\.R$", full.names = TRUE)
    for (f in fixture_files) {
      source(f)
    }
  }
})
