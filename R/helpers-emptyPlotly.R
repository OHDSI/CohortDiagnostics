emptyPlot <- function(title = NULL){
  p <- plotly::plotly_empty(type = "scatter", mode = "markers") %>%
    plotly::config(
      displayModeBar = FALSE
    ) %>%
    plotly::layout(
      title = list(
        text = title,
        yref = "paper",
        y = 0.5
      )
    )
  return(p)
} 

# takes a vector of 'text' and add <br> every 'length' characters
addTextBreaks <- function(
  text, 
  length
){
  
  textBreakVector <- c()
  
  for(textVal in text){
  textBreak <- ""
  reps <- 1:ceiling(nchar(textVal)/length)
  for(repsI in reps){
    space <- substr(textVal, (repsI-1)*length, (repsI-1)*length) == ' '
    if(space){
      breakText <- '<br>'
    } else{
      breakText <- '<br>-'
    }
    textBreak <- paste0(textBreak, ifelse(repsI==1, '', breakText),substr(textVal, (repsI-1)*length+1, min(nchar(textVal), (repsI)*length)))
  }
  textBreakVector <- c(textBreakVector, textBreak)
  }
  
return(textBreakVector)
}
