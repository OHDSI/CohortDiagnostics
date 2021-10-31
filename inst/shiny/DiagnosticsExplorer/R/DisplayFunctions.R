truncateStringDef <- function(columns, maxChars) {
  list(targets = columns,
       render = DT::JS(
         sprintf(
           "function(data, type, row, meta) {\n
      return type === 'display' && data != null && data.length > %s ?\n
        '<span title=\"' + data + '\">' + data.substr(0, %s) + '...</span>' : data;\n
     }",
           maxChars,
           maxChars
         )
       ))
}

minCellCountDef <- function(columns) {
  list(
    targets = columns,
    render = DT::JS(
      "function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }"
    )
  )
}

minCellPercentDef <- function(columns) {
  list(
    targets = columns,
    render = DT::JS(
      "function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return (100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
    return '<' + Math.abs(100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
  }"
    )
  )
}

minCellRealDef <- function(columns, digits = 1) {
  list(targets = columns,
       render = DT::JS(
         sprintf(
           "function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }",
           digits,
           digits
         )
       ))
}

styleAbsColorBar <-
  function(maxValue,
           colorPositive,
           colorNegative,
           angle = 90) {
    DT::JS(
      sprintf(
        "isNaN(parseFloat(value))? '' : 'linear-gradient(%fdeg, transparent ' + (%f - Math.abs(value))/%f * 100 + '%%, ' + (value > 0 ? '%s ' : '%s ') + (%f - Math.abs(value))/%f * 100 + '%%)'",
        angle,
        maxValue,
        maxValue,
        colorPositive,
        colorNegative,
        maxValue,
        maxValue
      )
    )
  }

copyToClipboardButton <-
  function(toCopyId,
           label = "Copy to clipboard",
           icon = shiny::icon("clipboard"),
           ...) {
    script <- sprintf(
      "
  text = document.getElementById('%s').textContent;
  html = document.getElementById('%s').innerHTML;
  function listener(e) {
    e.clipboardData.setData('text/html', html);
    e.clipboardData.setData('text/plain', text);
    e.preventDefault();
  }
  document.addEventListener('copy', listener);
  document.execCommand('copy');
  document.removeEventListener('copy', listener);
  return false;",
      toCopyId,
      toCopyId
    )
    
    tags$button(type = "button",
                class = "btn btn-default action-button",
                onclick = script,
                icon,
                label,
                ...)
  }

# convertMdToHtml <- function(markdown) {
#   markdown <- gsub("'", "%sq%", markdown)
#   mdFile <- tempfile(fileext = ".md")
#   htmlFile <- tempfile(fileext = ".html")
#   SqlRender::writeSql(markdown, mdFile)
#   rmarkdown::render(
#     input = mdFile,
#     output_format = "html_fragment",
#     output_file = htmlFile,
#     clean = TRUE,
#     quiet = TRUE
#   )
#   html <- SqlRender::readSql(htmlFile)
#   unlink(mdFile)
#   unlink(htmlFile)
#   html <- gsub("%sq%", "'", html)
#   
#   return(html)
# }


getCsvFileNameWithDateTime <- function(string) {
  date <-
    stringr::str_replace_all(Sys.Date(),
                             pattern = "-",
                             replacement = "")
  time <-
    stringr::str_split(string = Sys.time(),
                       pattern = " ",
                       n = 2)[[1]][2]
  timeArray <-
    stringr::str_split(string = time,
                       pattern = ":",
                       n = 3)
  return(paste(
    string,
    "_",
    date,
    "_",
    timeArray[[1]][1],
    timeArray[[1]][2],
    ".csv",
    sep = ""
  ))
}


addInfo <- function(item, infoId) {
  infoTag <- tags$small(
    class = "badge pull-right action-button",
    style = "padding: 1px 6px 2px 6px; background-color: steelblue;",
    type = "button",
    id = infoId,
    "i"
  )
  item$children[[1]]$children <-
    append(item$children[[1]]$children, list(infoTag))
  return(item)
}

createShinyBoxFromOutputId <- function(outputId) {
  shinydashboard::box(
    title = "Cohort Details",
    status = "warning",
    width = "100%",
    tags$div(style = "max-height: 60px; overflow-y: auto",
             shiny::uiOutput(outputId = outputId))
  )
}


createShinyBoxWithSplitForTwoOutputIds <- function(outputIdCohort,
                                                   outputIdDatasource) {
  shinydashboard::box(
    # title = "Reference",
    status = "warning",
    width = "100%",
    tags$div(style = "max-height: 60px; overflow-y: auto",
             tags$table(style = "width: 100% !important",
                        tags$tr(
                          tags$td(style = "width: 60% !important", valign = "top",
                            tags$b("Cohort: "),
                            shiny::uiOutput(outputId = outputIdCohort)
                          ),
                          tags$td(style = "width: 40% !important", valign = "top",
                            tags$b("Data source: "),
                            shiny::uiOutput(outputId = outputIdDatasource)
                          )
                        )))
  )
}
