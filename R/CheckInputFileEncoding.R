# Copyright 2021 Observational Health Data Sciences and Informatics
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

#' Check character encoding of input file
#'
#' @description
#' For its input files, CohortDiagnostics only accepts UTF-8 or ASCII character encoding. This
#' function can be used to check whether a file meets these criteria.
#'
#' @param fileName  The path to the file to check
#'
#' @return
#' Throws an error if the input file does not have the correct encoding.
#'
checkInputFileEncoding <- function(fileName) {
  encoding <- readr::guess_encoding(file = fileName, n_max = min(1e7))
  
  if (!encoding$encoding[1] %in% c("UTF-8", "ASCII")) {
    stop(
      "Illegal encoding found in file ",
      basename(fileName),
      ". Should be 'ASCII' or 'UTF-8', found:",
      paste(
        paste0(encoding$encoding, " (", encoding$confidence, ")"),
        collapse = ", "
      )
    )
  }
  invisible(TRUE)
}