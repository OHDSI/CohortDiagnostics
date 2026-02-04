
#' Result Table Viewer
#'
#' @param id string
#' @param boxTitle the title added to the box 
#' @family Utils
#' @return shiny module UI
#' 
#' @export
resultTableViewer <- function(
    id = "result-table",
    boxTitle = 'Table'
    ) {
  ns <- shiny::NS(id)
  shiny::div(# UI
    shinydashboard::box(
      width = "100%", 
      collapsible = TRUE,
      title = shiny::span(shiny::icon("table"), boxTitle),
      shiny::fluidPage(
        shiny::fluidRow(
          shiny::column(
            width = 7,
            shiny::uiOutput(ns("columnSelector"))
            ),
          shiny::column(
            width = 2,
            shiny::downloadButton(
              ns('downloadDataFull'),
              label = "Download (Full)",
              icon = shiny::icon("download")
            )
          ),
          
          shiny::column(
            width = 3,
            shiny::column(
              width = 2,
              shiny::uiOutput(outputId = ns('filterButton'))
            )
          )
        ),
        
        shiny::fluidRow(
          shinycssloaders::withSpinner(
            reactable::reactableOutput(
              outputId = ns("resultData"),
              width = "100%"
            )
           )
          )
      )
    )
  )
}






#' Result Table Server
#'
#' @param id string, table id must match resultsTableViewer function
#' @param df reactive that returns a data frame
#' @param colDefsInput named list of reactable::colDefs
#' @param columnGroups list specifying how to group columns 
#' @param details The details of the results such as cohort names and database names used when downloading table
#' @param selectedCols string vector of columns the reactable should display to start by default. Defaults to ALL if not specified.
#' @param elementId optional string vector of element Id name for custom dropdown filtering if present in the customColDef list. Defaults to NULL.
#' @param addActions add a button row selector column to the table to a column called 'actions'.  
#'                   actions must be a column in df
#' @param downloadedFileName string, desired name of downloaded data file. can use the name from the module that is being used
#' @param groupBy The columns to group by 
#' @param selection NULL/single/multiple (whether to enable table row selection)
#' @param getSelected A reactive that triggers an even to extract the selected rows of the table
#' @param setSelected A reactive that triggers an even to set the selected rows of the table
#' @param selectedRowId The selected rows
#' @param showPageSizeOptions Show page size options?
#' @param pageSizeOptions Page size options for the table. Defaults to 10, 25, 50, 100.
#' @param defaultPageSize Default page size for the table. Defaults to 10.
#' 
#' @return shiny module server
#' @family {Utils}
resultTableServer <- function( # add column for selected columns as a reactive
    id = "result-table", #string
    df, #data.frame
    colDefsInput = NULL,
    columnGroups = NULL,
    details = data.frame(), # details about the data.frame such as target and database name
    selectedCols = NULL,
    elementId = NULL,
    addActions = NULL,
    downloadedFileName = NULL,
    groupBy = NULL,
    selection = NULL,
    getSelected = shiny::reactiveVal(0),
    setSelected = shiny::reactiveVal(0),
    selectedRowId = shiny::reactiveVal(0),
    showPageSizeOptions = TRUE,
    pageSizeOptions = c(10,25,50,500),
    defaultPageSize = 10
) #list of colDefs, can use checkmate::assertList, need a check that makes sure names = columns) {
  shiny::moduleServer(
    id,
    function(input, output, session) {
      
      # convert a data.frame to a reactive
      if(!inherits(df, 'reactive')){
        df <- shiny::reactiveVal(df)
      }
      
      # add action column to df()
      dfWithActions <- shiny::reactive({
        cbind(actions = rep("", nrow(df())),
              df()
        )
      })
      
      # add a new entry to colDefs with an action dropdown menu
      # add a onClick action
      if(!is.null(addActions)){
        
        onClickText <- paste0(
          "function(rowInfo, column) {",
          paste("if(column.id == 'actions'){
      Shiny.setInputValue('",session$ns(paste0('action_index')),"', { index: rowInfo.index + 1 }, { priority: 'event' })
    }", collapse = ' ', sep = ''),
          "}"
        )
        onClick <- reactable::JS(onClickText)
        
        colDefsInput <- addTableActions(
          colDefsInput = colDefsInput,
          addActions = addActions,
          session = session
        )
        
      } else{
        onClick <- "select" #NULL
        
        # add action colDef with show = FALSE
        colDefsInput[[length(colDefsInput) + 1 ]] <- reactable::colDef(
          show = FALSE
        ) 
        names(colDefsInput)[length(colDefsInput)] <- 'actions'
      }
      
      # initialize the reactables
      actionCount <- shiny::reactiveVal(0)
      actionIndex <- shiny::reactiveVal(0)
      actionType <- shiny::reactiveVal('none')

      # find the column info 
      columnInfo <- shiny::reactive(
        if(!is.null(df())){
          extractColumnRelations(
            data = df(),
            columnDef = colDefsInput, 
            columnGroups = columnGroups,
            selectedCols = selectedCols
          )} else{
            NULL
          }
        )
      
      # reactable to store the column selection options
      columnsToSelectOptions <- shiny::reactive(
        if(!is.null(columnInfo())){
          unique(columnInfo()$friendlyName[columnInfo()$show])
        } else{
          NULL
        }
      )
      
      # reactable to save the initially selected columns
      tableSelected <- shiny::reactive(
        if(!is.null(columnInfo())){
        unique(columnInfo()$friendlyName[columnInfo()$show & columnInfo()$selectedInitially])
        } else{
          NULL
        }
        )
      
      # react to df, columnInfo and input
      # MAIN REACTIVE
      colDefsInputReactive <- shiny::reactive({
        colDefsInputTemp <- colDefsInput
        removeExtras <- names(colDefsInput)[!names(colDefsInput) %in% names(dfWithActions())]
        # remove extra columnDefs
        if(length(removeExtras) > 0){
          for(extra in removeExtras){
            colDefsInputTemp[extra] <- NULL
          }
        }
        
        columnIdsToDisplay <- columnInfo()$columnId[columnInfo()$friendlyName %in% input$dataCols]
        
        # update the colDef to show only selected columns
        for(col in names(colDefsInputTemp)){
          colDefsInputTemp[[col]]$show <- col %in% c(ifelse(is.null(addActions), '','actions'), columnIdsToDisplay)
        }
        
        return(colDefsInputTemp)
        
        })
      
      # code for the column selection
      output$columnSelector <- shiny::renderUI({
        
        shinyWidgets::pickerInput(
          inputId = session$ns('dataCols'),
          label = 'Select Columns to Display: ',
          choices = columnsToSelectOptions(), 
          selected = tableSelected(),
          choicesOpt = list(style = rep_len("color: black;", 999)),
          multiple = TRUE,
          options = shinyWidgets::pickerOptions(
            actionsBox = TRUE,
            liveSearch = TRUE,
            size = 10,
            liveSearchStyle = "contains",
            liveSearchPlaceholder = "Type here to search",
            virtualScroll = 50
          ),
          width = "75%"
        )
        
      })

      
#use fuzzy text matching for global table search
fuzzySearch <- reactable::JS('function(rows, columnIds, filterValue) {

  // Create a case-insensitive RegEx pattern that performs a fuzzy search.
  const pattern = new RegExp(filterValue, "i");
  
  return rows.filter(function(row) {
    return columnIds.some(function(columnId) {
      return pattern.test(row.values[columnId]);
    });
  });
}')


      output$resultData <- reactable::renderReactable({
        # Display message when dat is empty
        shiny::validate(shiny::need(hasData(df()), "No data for selection"))
        # set row height based on nchar of table
        height <- NULL
        maxMinWidth <- max(unlist(lapply(colDefsInputReactive(), function(x) x$minWidth)))
        maxMinWidth <- ifelse(is.finite(maxMinWidth),maxMinWidth, 40)
        if(max(apply(df(), 1, function(x) max(nchar(x))), na.rm = TRUE) < maxMinWidth*3){
          if(!is.null(addActions)){
            height <- 40*3#length(addActions)
          }
        }
                reactable::reactable(
                  data = dfWithActions(),
                  columns = colDefsInputReactive(),
                  columnGroups = columnGroups,
                  onClick = onClick,
                  groupBy = groupBy,
                  selection = selection,
                  defaultSelected = shiny::reactive({if(max(selectedRowId()) == 0){NULL}else{selectedRowId()}})(),
                  #these can be turned on/off and will overwrite colDef args
                  sortable = TRUE,
                  resizable = TRUE,
                  filterable = TRUE,
                  searchable = TRUE,
                  searchMethod = fuzzySearch,
                  showPageSizeOptions = showPageSizeOptions,
                  pageSizeOptions = pageSizeOptions,
                  defaultPageSize = defaultPageSize,
                  outlined = TRUE,
                  showSortIcon = TRUE,
                  striped = TRUE,
                  highlight = TRUE,
                  rowStyle = list(
                    height = height
                    ),
                  elementId = elementId
                )
        })
      
      # update selected rows when getSelected changes
      shiny::observeEvent(getSelected(), {
        selectedTemp <- reactable::getReactableState(outputId = 'resultData', name = 'selected')
        if(is.null(selectedTemp)){
          selectedRowId(0)
          reactable::updateReactable(
            outputId = 'resultData', 
            selected = NA
          )
        } else{
          selectedRowId(selectedTemp)
          
          # code to set the row if selectedRow() is not NULL
          #reactable::updateReactable(
         #   outputId = 'resultData', 
         #   selected = selectedRowId()
         # )
        }
      })
      
      # add listener that update the table selected rows if 
      # setSelected() updates
      shiny::observeEvent(
        eventExpr = setSelected(), {
          
          if(max(selectedRowId()) == 0){
            # do nothing
          } else{
            # code to set the row if selectedRow() is not NULL
            reactable::updateReactable(
               outputId = 'resultData', 
               selected = selectedRowId()
             )
          }
        }
      )
      
      
      output$filterButton <- shiny::renderUI(
        shiny::tags$button(
          class = "btn btn-default",
          shiny::tags$i(class = "fas fa-download", role = "presentation", "aria-label" = "download icon"),
          "Download (Filtered)",
          onclick = sprintf("Reactable.downloadDataCSV('%s', '%s')", elementId, paste0('result-data-filtered-',downloadedFileName, Sys.Date(), '.csv', sep = ''))
        )
      )
      
      # download full data button
      output$downloadDataFull <- shiny::downloadHandler(
        filename = function() {
          paste('result-data-full-', downloadedFileName, Sys.Date(), '.xlsx', sep = '')
        },
        content = function(con) {
          wb <- openxlsx::buildWorkbook(x = list(
            details = details,
            results = df()
          ))
          openxlsx::saveWorkbook(wb = wb, file = con)
        }
      )
      
      # capture the actions
      shiny::observeEvent(input$action_index, {
        actionIndex(input$action_index)
      })
      
      shiny::observeEvent(input$action_type, {
          # update type
          actionType(input$action_type$value)
          # update count
          actionCount(input$action_type$seed)
        })
      
      return(
        list(
          actionType = actionType, 
          actionIndex = actionIndex,
          actionCount = actionCount 
        )
      )

    })


# HELPERS
addTableActions <- function(
    colDefsInput,
    addActions,
    session
){
  
  args <- list(
    label = "Actions",
    status = "primary",
    circle = FALSE,
    width = "300px",
    margin = "5px",
    inline = T
  )
  
  args <- append(
    args, 
    lapply(
    X = addActions,
    FUN = function(x){
      shiny::actionLink(
        inputId = session$ns(x),
        label = paste0('View ',x),
        icon = shiny::icon("play"),
        onClick = reactable::JS(
          paste0(
            "function() {
                  Shiny.setInputValue('",session$ns(paste0('action_type')),"', { value: '",x,"', seed: Math.random()})
                  }"
          )
        )
      )
    })
  )
  
  tableActionfunction <- function(){ 
  
  cellFunction <- do.call(
    args = args,
    what = shinyWidgets::dropdownButton
  )
  return(cellFunction)
  }
  
  # add the actions dropdown
  colDefsInput[[length(colDefsInput) + 1 ]] <- reactable::colDef(
    name = "",
    sortable = FALSE,
    filterable = FALSE,
    minWidth = 150,
    cell = tableActionfunction
  ) 
  names(colDefsInput)[length(colDefsInput)] <- 'actions'
  
  return(colDefsInput)
}


#tooltip function
withTooltip <- function(value, tooltip, ...) {
  shiny::div(
    style = "text-decoration: underline; text-decoration-style: dotted; cursor: help",
    tippy::tippy(value, tooltip, ...)
  )
}


extractColumnRelations <- function(
    data,
    columnDef,
    columnGroups,
    selectedCols = NULL
    ){
  
  columnDf <- NULL
  
  if(!is.null(data)){
    if(nrow(data) > 0 ){
      columnDf <- data.frame(
        columnId = colnames(data)
      )
      
      if(is.null(columnDef)){
        columnDf$show <- TRUE
        columnDf$friendlyName <- columnDf$columnId
      } else {
        
        columnDf <- columnDf %>% 
          dplyr::left_join(
            data.frame(
              columnId = names(columnDef),
              show = unlist(lapply(columnDef, function(x) ifelse(is.null(x$show), TRUE, x$show))),
              friendlyName = unlist(lapply(columnDef, function(x) ifelse(is.null(x$name), NA ,x$name) ))
            ), 
            by = 'columnId'
          )
        
        if(length(is.na(columnDf$show)) > 0){
          columnDf$show[is.na(columnDf$show)] <- TRUE
        }
        if(length(is.na(columnDf$friendlyName)) > 0){
          columnDf$friendlyName[is.na(columnDf$friendlyName)] <- columnDf$columnId[is.na(columnDf$friendlyName)]
        }
      }
      
      if(!is.null(selectedCols)){
        columnDf$selectedInitially <- columnDf$columnId %in% selectedCols
      } else{
        columnDf$selectedInitially <- TRUE
      }
    
    } 
  }
  
  return(columnDf)
  
}