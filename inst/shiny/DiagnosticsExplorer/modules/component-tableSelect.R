# create a input selector 
# that shows the options as columns and 

# inputs: table to display
# input columns 
# output selected row id

tableSelectionViewer <- function(id = "input-selection") {
  ns <- shiny::NS(id)
  
  shiny::div(
    # UI for inputs - has a button that activates a model with the options as a table
    shiny::uiOutput(ns('selectionInput'))
    
  )
  
}


tableSelectionServer <- function(
    id, 
    table, # must be reactive
    selectedRowId, # must be reactive
    helpText = 'Click the button to make your selection',
    selectMultiple = FALSE,
    inputColumns = NULL,
    displayColumns = inputColumns,
    elementId = NULL,
    selectButtonText = 'Select Option',
    tableReset = shiny::reactive(0),
    groupBy = NULL,
    columnGroups = NULL
) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
      # defaults
      ##selectedRow <- shiny::reactiveVal(NULL)
      selection <- ifelse(selectMultiple, 'multiple','single')
      selectButtonText <- shiny::reactiveVal(selectButtonText)
      helpTextReactive <- shiny::reactiveVal(helpText)
      icon <- shiny::reactiveVal('plus')
      
      # reset if table changes - TODO remove this?
      shiny::observeEvent(tableReset(),{
        icon('plus')
        helpTextReactive(helpText)
        output$selectedRow <- NULL
      })
      
      # create the main UI
      output$selectionInput <- shiny::renderUI(
        shinydashboard::box(
          collapsible = TRUE,
          title = shiny::actionButton(
            inputId = session$ns('openModal'), 
            icon = shiny::icon(icon()),
            label = selectButtonText()
          ),
          width = "100%",
          shiny::helpText(helpTextReactive()),
          shiny::uiOutput(session$ns('selectedRow'))
        )
      )
      
      # when the selection button is pressed open a modal
      # with the table to select from
      shiny::observeEvent(
        eventExpr = input$openModal, {
          
          shiny::showModal(
            shiny::modalDialog(
              size = 'l', 
              easyClose = TRUE,
            title = "Select row/s", 
            shiny::helpText('Select row/s of interest by clicking on the selector and then scroll down to the bottom of the table to hit the "Select" button and confirm your selection.  Hitting the "Select" button will close the modal.'),
            
            resultTableViewer(
              id = session$ns("input-table"), 
              boxTitle = 'Options'
            ),

            footer = shiny::actionButton(
              inputId = session$ns("confirmInput"), 
              label = 'Select'
              )
            )
          )
          
          # code to update the selected rows
          # using setSelected()
          oldSetSelected <- setSelected()
          setSelected(oldSetSelected + 1)
        }
        )
      
      setSelected <- shiny::reactiveVal(0)
      getSelected <- shiny::reactiveVal(0)
       resultTableServer(
         id = "input-table", #string
         df = table, #data.frame
         colDefsInput = inputColumns,
         columnGroups = columnGroups,
         details = data.frame(), # details about the data.frame such as target and database name
         selectedCols = NULL,
         elementId = elementId,
         addActions = NULL,
         downloadedFileName = NULL,
         groupBy = groupBy,
         selection = selection,
         getSelected = getSelected,
         selectedRowId = selectedRowId,
         setSelected = setSelected,
         showPageSizeOptions = TRUE,
         pageSizeOptions = c(5,25,50,500),
         defaultPageSize = 5 
       )
       
       # when modal button is clicked update the selected row and 
       # remove model
       shiny::observeEvent(input$confirmInput,{
         shiny::removeModal() # close the modal
         
         # change getSelected to trigger selectedRowId to update
         oldCount <- getSelected()
         getSelected(oldCount+1) 
       }
       )
       
       
       # observe the row change to update the selected
       # need this for it to work across servers
       shiny::observeEvent(selectedRowId(), {
         print(session$ns('In tableSelect'))
         print(selectedRowId())
         
         if(sum(selectedRowId()) == 0){
           icon('plus')
           helpTextReactive(helpText)
           output$selectedRow <- NULL
         } else{
           if(nrow(table()[selectedRowId(),]) > 0){
             # update the icon if a row is selected
             icon('redo')
             helpTextReactive("")
             
             output$selectedRow <- shiny::renderUI(
               shiny::div(
                 #shiny::h4('Selected: '),
                 reactable::reactable(
                   data = table()[selectedRowId(),], 
                   columns = displayColumns,
                   sortable = FALSE,
                   filterable = FALSE, 
                   searchable = FALSE, 
                   compact = TRUE,
                   pagination = FALSE,
                   showPageInfo = FALSE
                 )
               )
             )
             
           } else{
             icon('plus')
             helpTextReactive(helpText)
             output$selectedRow <- NULL
           }
         }
       }
         
       )
       
      
    }
  )
}