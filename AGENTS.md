# AGENTS.md

Instructions for AI agents (and humans) working in the **CohortDiagnostics** R package.

## Project overview

CohortDiagnostics is an R package in the OHDSI/HADES ecosystem. Source lives in `R/`,
tests in `tests/testthat/`, documentation in `man/` and `vignettes/`. `renv` is used for
dependency management.

- Load the package during development with `pkgload::load_all()` or `devtools::load_all()`.
- Regenerate documentation with `devtools::document()` after editing roxygen comments.

## Code style and linting

The package follows the [tidyverse style guide](https://style.tidyverse.org/) and is checked
with `lintr` (config in `.lintr`) and formatted with `styler`.

```r
# Lint the whole package
lintr::lint_package()

# Lint a single file
lintr::lint("R/SomeFile.R")

# Auto-format the package (or a single file) in place
styler::style_pkg()
styler::style_file("R/SomeFile.R")
```

Notes:
- Code must be formatted with `styler` before committing. CI fails a PR if
  `styler::style_pkg(dry = "fail")` would change files.
- CI lints **only the files changed in a pull request**, so pre-existing lints in
  untouched files will not block a PR. Fix lints in files you touch.
- The config allows both `camelCase` and `snake_case` names, explicit `return()`, and the
  magrittr `%>%` pipe (keep pipes consistent within a file).
- Never commit `print()`, `browser()`, or `debug()` statements in `R/`.

### Pre-commit hooks

A `.pre-commit-config.yaml` runs `styler`, `lintr`, and whitespace/artifact checks on
staged files.

```bash
pip install pre-commit        # or: brew install pre-commit
pre-commit install            # enable the git hook in this clone
pre-commit run --all-files    # run all hooks manually
```

The R hooks install their own R environment and therefore require a working package
compilation toolchain (Xcode/CLT on macOS, build-essential on Linux).

## Testing

Unit tests are fast and require no database. Integration tests require a database
(e.g. Eunomia/SQLite) and are opt-in.

```r
# Unit tests only (fast)
pkgload::load_all()
testthat::test_local(filter = "unit-")

# Full suite (includes integration tests when enabled)
Sys.setenv(INTEGRATION_TESTS = "TRUE")
devtools::test()
```

```bash
# From the command line
Rscript -e 'pkgload::load_all(quiet = TRUE); testthat::test_local(filter = "unit-")'
```

See `tests/testthat/TESTING.md` for the full testing guide, fixtures, and mocking helpers.

### SqlRender and `load_all()` / `devtools::test()`

`SqlRender::loadRenderTranslateSql()` locates the package's SQL files with
`system.file()`, which does not resolve `inst/` under `devtools::load_all()` or
`devtools::test()`. Tests that run diagnostics then fail with
`Cannot find '<file>.sql' in the 'sql' or 'sql/sql_server' folder`.

Work around it with a symbolic link at the package root (already ignored via
`/sql/` in `.gitignore`, so it is never committed):

```bash
ln -s inst/sql sql      # macOS/Linux; on Windows use a shortcut/junction to inst\sql
```

In CI, where the package is not installed from the working tree, create the same
link (or install the package and test the installed copy) before running tests.

### Regression snapshot workflow (required)

Before starting a feature or bug fix, take a **testing snapshot** to establish a baseline:

1. Run the test suite before making changes and record the result (pass/fail counts).
2. Make the change.
3. Re-run the same suite and compare against the baseline to confirm no regressions.

Do not treat pre-existing failures as caused by your change, but do not silently ignore
new ones either — report both.

## Git and workflow rules

- **Never `git push` (or open/update a PR) without explicitly asking the user first.**
  Committing locally is fine when asked to "commit"; pushing always needs confirmation.
- Never commit secrets, credentials, or large artifacts (`.RData`, `.rds`, `.Rhistory`).
- Create a feature branch for each change; do not commit directly to `main` or `develop`.
  Use descriptive names matching existing conventions, e.g. `features/<name>`,
  `fix-<name>`, or `issue-<number>`.
- Keep commits focused and write imperative commit messages, e.g.
  `Fix incremental mode on repeat runs`. Reference the issue number when relevant.
- Run `lintr`, `styler`, and the unit tests before committing.
