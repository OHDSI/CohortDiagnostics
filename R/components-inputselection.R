inputSelectionViewer <- function(id = "input-selection") {
   ns <- shiny::NS(id)
  
   shiny::div(
     
   # UI for inputs
   # summary table
   shinydashboard::box(
     collapsible = TRUE,
     title = "Options",
     width = "100%",
     shiny::uiOutput(ns("inputs"))
   ),
   
   # displayed inputs
   shiny::conditionalPanel(
     condition = "input.generate != 0",
     ns = ns,
     
     shinydashboard::box(
       status = 'warning', 
       width = "100%",
       title = 'Selected: ', 
       collapsible = T,
       shiny::uiOutput(ns("inputsText"))
     )
   )
   
   )
   
}

createInputSetting <- function(
    rowNumber,                           
    columnWidth = 4,
    varName = '',
    inputReturn = T,
    uiFunction = 'shinyWidgets::pickerInput',
    uiInputs = list(
      label = 'Input: ',
      choices = list(),
      multiple = F,
      options = shinyWidgets::pickerOptions()
    ),
    updateFunction = NULL,
    collapse = F,
    namesCallback = NULL
    ){
  
  result <- list(
    rowNumber = rowNumber,
    columnWidth = columnWidth,
    varName = varName,
    inputReturn = inputReturn,
    uiFunction = uiFunction,
    uiInputs = uiInputs,
    updateFunction = updateFunction,
    collapse = collapse,
    namesCallback = namesCallback
  )
  
  class(result) <- 'inputSetting'
  return(
    result
  )
}

inputSelectionServer <- function(
    id, 
    inputSettingList
) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
    if(inherits(inputSettingList, 'inputSetting')){
      inputSettingList <- list(inputSettingList)
    }
      
      rowNumbers <- unlist(lapply(inputSettingList, function(x){x$rowNumber}))
      inputNames <- unlist(lapply(inputSettingList, function(x){x$varName}))
      rows <- list()
      for(i in 1:max(rowNumbers)){
        rows[[i]] <- shiny::fluidRow(
          lapply(which(rowNumbers == i), function(x){
            
            inputs <- inputSettingList[[x]]$uiInputs
            if(inputSettingList[[x]]$inputReturn){
              # if using a function that has no return (e.g., div) set 
              # inputReturn = F
              inputs$inputId <- session$ns(paste0('input_',x))
            }
            
            shiny::column(
              width = inputSettingList[[x]]$columnWidth,
              do.call(eval(parse(text = inputSettingList[[x]]$uiFunction)), inputs)
            )
          }
          )
        )
      }
      rows[[length(rows)+1]] <- shiny::actionButton(
        inputId = session$ns('generate'), 
        label = 'Generate Report'
      )
      
      # add reset here
      rows[[length(rows)+1]] <- shiny::actionButton(
        inputId = session$ns('reset'), 
        label = 'Reset'
      )
      
      output$inputs <- shiny::renderUI({
        shiny::fluidPage(rows)
        })
        
      selectedInput <- shiny::reactiveVal()
      selectedInputText <- shiny::reactiveVal()
      output$inputsText <- shiny::renderUI(selectedInputText())
      
      # when generate is pressed update the selected values and text
      shiny::observeEvent(
        eventExpr = input$generate,
        {
          
          # get the input values and store in reactiveval
          inputList <- lapply(
            1:length(inputNames), 
            function(x){
              input[[paste0('input_', x)]]
          }
          )
          names(inputList) <- inputNames
          selectedInput(inputList)
          
          # create the text output
          
          otext <- list()
          for(i in 1:max(rowNumbers)){
            otext[[i]] <- shiny::fluidRow(
              lapply(which(rowNumbers == i), function(x){
                shiny::column(
                  width = inputSettingList[[x]]$columnWidth,
                  shiny::tags$b(paste0(inputSettingList[[x]]$uiInputs$label)),
                  if(!is.null(inputSettingList[[x]]$uiInputs$choices)){
                    inputVar <- paste0('input_',x)
                    if (!is.null(inputSettingList[[x]]$namesCallback)) {
                      namesVec <- inputSettingList[[x]]$namesCallback(input[[inputVar]])
                    } else {
                      # adding below incase a vector with no names is used
                      if(is.null(names(inputSettingList[[x]]$uiInputs$choices))){
                        names(inputSettingList[[x]]$uiInputs$choices) <- inputSettingList[[x]]$uiInputs$choices
                      }
  
                      namesVec <- names(inputSettingList[[x]]$uiInputs$choices)[inputSettingList[[x]]$uiInputs$choices %in% input[[inputVar]]]
                      
                    }
                    # add selections on new row unless collapse is F
                    if(!inputSettingList[[x]]$collapse){
                      shiny::HTML(
                        paste("<p>", namesVec, '</p>')
                      )
                      } else{
                        paste(namesVec, collapse = ', ')
                      }
                  } else{
                    
                    # add selections on new row unless collapse is F
                    if(!inputSettingList[[x]]$collapse){
                      shiny::HTML(
                        paste("<p>", input[[paste0('input_',x)]], '</p>')
                      )
                    } else{
                      paste(input[[paste0('input_',x)]], collapse = ', ')
                    }
                  }
                )
              }
              )
            )
          }
          selectedInputText(shiny::div(otext))
        })
      
      
      # do the reset stuff
      shiny::observeEvent(
        eventExpr = input$reset,
        {
          # code to reset to default
          
          for(i in 1:length(inputSettingList)){
            if(!is.null(inputSettingList[[i]]$updateFunction)){
              
              # need to test for non-picker inputs
              do.call(eval(parse(text = inputSettingList[[i]]$updateFunction)), 
                      list(
                        session = session, 
                        inputId = paste0('input_',i), 
                        selected = inputSettingList[[i]]$uiInputs$selected
                      ))
  
            }
          }
        })
      
      return(selectedInput)
          
    }
  )
}




# component module that takes a single row data.frame and returns the values 
# as string

inputSelectionDfViewer <- function(
    id = "input-selection-df",
    title = ''
) {
  ns <- shiny::NS(id)
  
  shiny::div(
    shinydashboard::box(
      title = title,
      status = "warning",
      width = "100%", 
      collapsible = T,
      shiny::uiOutput(outputId = ns("dataFrameSelection"))
    )
  )
}


inputSelectionDfServer <- function(
    id, 
    dataFrameRow,
    ncol = 2
) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
      otext <- shiny::reactive({
        if(is.null(dataFrameRow())){
          return('')
        } else{
          otext <- list()
          inputNames <- colnames(dataFrameRow())
          
          inputValues <- dataFrameRow()
          
          rows <- ceiling((1:length(inputNames))/ncol)
          
          for(rowInd in unique(rows)){
            otext[[rowInd]] <- shiny::fluidRow(
              lapply(which(rows == rowInd), function(x){
                shiny::column(
                  width = floor(12/ncol),
                  shiny::tags$b(paste0(inputNames[x]," :")),
                  inputValues[x]
                )
              }
              )
            )
          }
          
          return(otext)
        }
      })
      
      output$dataFrameSelection <- shiny::renderUI(
        shiny::div(otext())
      )
      
    }
  )
}
