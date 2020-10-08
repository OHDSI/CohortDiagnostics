camelCaseToSnakeCase <- function(string) {
  string <- gsub("([A-Z])", "_\\1", string)
  string <- tolower(string)
  string <- gsub("([a-z])([0-9])", "\\1_\\2", string)
  return(string)
}


camelCaseToTitleCase <- function(string) {
  string <- gsub("([A-Z])", " \\1", string)
  string <- gsub("([a-z])([0-9])", "\\1 \\2", string)
  substr(string, 1, 1) <- toupper(substr(string, 1, 1))
  return(string)
}


truncateStringDef <- function(columns, maxChars) {
  list(
    targets = columns,
    render = DT::JS(sprintf("function(data, type, row, meta) {\n
      return type === 'display' && data != null && data.length > %s ?\n
        '<span title=\"' + data + '\">' + data.substr(0, %s) + '...</span>' : data;\n
     }", maxChars, maxChars))
  )
}

minCellCountDef <- function(columns) {
  list(
    targets = columns,
    render = DT::JS("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }")
  )
}

minCellPercentDef <- function(columns) {
  list(
    targets = columns,
    render = DT::JS("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return (100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
    return '<' + Math.abs(100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
  }")
  )
}

minCellRealDef <- function(columns, digits = 1) {
  list(
    targets = columns,
    render = DT::JS(sprintf("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }", digits, digits))
  )
}

styleAbsColorBar <- function(maxValue, colorPositive, colorNegative, angle = 90) {
  DT::JS(sprintf("isNaN(parseFloat(value))? '' : 'linear-gradient(%fdeg, transparent ' + (%f - Math.abs(value))/%f * 100 + '%%, ' + (value > 0 ? '%s ' : '%s ') + (%f - Math.abs(value))/%f * 100 + '%%)'", 
                 angle, maxValue, maxValue, colorPositive, colorNegative, maxValue, maxValue))
}

sumCounts <- function(counts) {
  result <- sum(abs(counts))
  if (any(counts < 0)) {
    return(-result)
  } else {
    return(result)
  }
  
}
