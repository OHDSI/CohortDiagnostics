readLog <- function(path) {
  df <- utils::read.csv(path, sep = "\t", header = FALSE)
  names(df) <- c("time", "thread", "level", "package", "task", "message")
  
  df %>%
    dplyr::tibble() %>%
    dplyr::mutate(
      time = as.POSIXct(x = .data$time, format = "%Y-%m-%d %H:%M:%OS"),
      time_normalized = difftime(.data$time, max(.data$time), units = "secs"),
      time_normalized = .data$time_normalized - min(.data$time_normalized)
    )
}

parseLog <- function(log) {
  dplyr::bind_rows(
    log[grep(pattern = "took \\d{1,}(\\.\\d{1,})? secs$", log$message), ] |>
      dplyr::mutate(
        duration = as.numeric(stringr::str_extract(
          string = .data$message,
          pattern = "\\d{1,}(\\.\\d{1,})?"
        ))
      ),
    
    log[grep(pattern = "took \\d{1,}(\\.\\d{1,})? mins$", log$message), ] |>
      dplyr::mutate(
        duration = as.numeric(stringr::str_extract(
          string = .data$message,
          pattern = "\\d{1,}(\\.\\d{1,})?"
        )) * 60
      ),
    
    log[grep(pattern = "took \\d{1,}(\\.\\d{1,})? hours$", log$message), ] |>
      dplyr::mutate(
        duration = as.numeric(stringr::str_extract(
          string = .data$message,
          pattern = "\\d{1,}(\\.\\d{1,})?"
        )) * 3600
      ),
    
    log[grep(pattern = "took \\d{1,}(\\.\\d{1,})? days$", log$message), ] |>
      dplyr::mutate(
        duration = as.numeric(stringr::str_extract(
          string = .data$message,
          pattern = "\\d{1,}(\\.\\d{1,})?"
        )) * 3600 * 24
      )
  )
}

#' plotLogFile
#'
#' @param logPath Path to the generated logfile 
#'
#' @return `ggplot`
#' @export
#'
#' @examples
#' logPath <- "./path/to/log.txt"
#' if (file.exists(logPath)) {
#'   plotLogFile(logPath)
#' }
plotLogFile <- function(logPath) {
  df <- readLog(logPath) |>
    parseLog()
  
  ggplot2::ggplot(data = df, mapping = ggplot2::aes(x = .data$task, y = .data$duration, fill = .data$package)) +
    ggplot2::geom_bar(stat = "identity") +
    ggplot2::theme_bw() +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, vjust = 1, hjust = 1))
}
