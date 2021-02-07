listFilesInGitHub <-
  function(phenotypeId,
           subFolder,
           repo = "OHDSI/PhenotypeLibrary",
           branch = "develop") {
    # Avoiding GitHub API to avoid rate limit:
    url <-
      sprintf("https://github.com/%s/tree/%s/inst/%s/%s",
              repo,
              branch,
              phenotypeId,
              subFolder)
    pageGet <- httr::GET(url)
    page <- httr::content(pageGet, as = "text")
    
    baseFileUrl <-
      sprintf("/%s/blob/%s/inst/%s/%s/",
              repo,
              branch,
              phenotypeId,
              subFolder)
    files <-
      stringr::str_extract_all(page, sprintf("%s.*\"", baseFileUrl))[[1]] %>%
      stringr::str_replace("\"$", "") %>%
      stringr::str_replace(baseFileUrl, "")
    files <- files[files != ".empty"]
    downloadLinks <-
      sprintf(
        "https://github.com/%s/raw/%s/inst/%s/%s/%s",
        repo,
        branch,
        phenotypeId,
        subFolder,
        files
      )
    html <- sprintf("<a href=\"%s\">%s</a>", downloadLinks, files)
    return(dplyr::tibble(
      file = files,
      downloadLink = downloadLinks,
      html = html
    ))
  }
