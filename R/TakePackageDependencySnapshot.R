# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Take a snapshot of the R environment
#'
#' @details
#' This function records all versions used in the R environment as used by runCohortDiagnostics.
#' This function was borrowed from OhdsiRTools
#'
#' @return
#' A data frame listing all the dependencies of the root package and their version numbers, in the
#' order in which they should be installed.
#'
takepackageDependencySnapshot <- function() {
  splitPackageList <- function(packageList) {
    if (is.null(packageList)) {
      return(c())
    } else {
      return(strsplit(
        gsub(
          "\\([^)]*\\)", "", gsub(" ", "", gsub("\n", "", packageList))
        ),
        ","
      )[[1]])
    }
  }

  fetchDependencies <-
    function(package,
             recursive = TRUE,
             level = 0) {
      description <- utils::packageDescription(package)
      packages <- splitPackageList(description$Depends)
      packages <- c(packages, splitPackageList(description$Imports))
      packages <-
        c(packages, splitPackageList(description$LinkingTo))
      # Note: if we want to include suggests, we'll need to consider circular references packages <-
      # c(packages, splitPackageList(description$Suggests))
      packages <- packages[packages != "R"]
      packages <- data.frame(
        name = packages,
        level = rep(
          level,
          length(packages)
        ),
        stringsAsFactors = FALSE
      )
      if (recursive && nrow(packages) > 0) {
        all <-
          lapply(packages$name,
            fetchDependencies,
            recursive = TRUE,
            level = level + 1
          )
        dependencies <- do.call("rbind", all)
        if (nrow(dependencies) > 0) {
          packages <- rbind(packages, dependencies)
          packages <- aggregate(level ~ name, packages, max)
        }
      }
      return(packages)
    }

  packages <-
    fetchDependencies("CohortDiagnostics", recursive = TRUE)
  packages <- packages[order(-packages$level), ]
  getVersion <- function(package) {
    return(utils::packageDescription(package)$Version)
  }
  versions <-
    sapply(c(packages$name, "CohortDiagnostics"), getVersion)
  snapshot <- data.frame(
    package = names(versions),
    version = as.vector(versions),
    stringsAsFactors = FALSE
  )
  s <- utils::sessionInfo()
  rVersion <- data.frame(
    package = "R",
    version = paste(s$R.version$major, s$R.version$minor, sep = "."),
    stringsAsFactors = FALSE
  )
  snapshot <- rbind(rVersion, snapshot)
  return(snapshot)
}
