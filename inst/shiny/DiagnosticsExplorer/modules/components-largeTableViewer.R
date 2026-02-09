#' Large Data Table
#' @export
#' @description
#' Large data table R6 class.
#'
#' Uses ResultModelManager::ConnectionHandler class to create paginating tables
#'
#' NOTE Only currently works with sqlite and postgresql database backends (probably redshift too)
#' as this method uses limit and offset for the queries
#'
#' Alternatively, you might want to subclass this class. For example, if your backend query is against an API such
#' as and ATLAS instance or ATHENA
#'
#' If subclassing use inheritance and treat this class as an interface to implement - implementing the methods:
#'
#'  get
#'
#' @field baseQuery         query string sql
#' @field countQuery        count query string (should match query). Can be auto generated with sub query (default) but
#'                          this will likely result in slow results
#' @field connectionHandler ResultModelManager connection handler to execute query inside
#' @family {LargeTables}
LargeDataTable <- R6::R6Class(
  classname = "LargeDataTable",
  public = list(
    connectionHandler = NULL,
    baseQuery = NULL,
    countQuery = NULL,
    #' initialize
    #'
    #' @param connectionHandler         ResultModelManager connectionHandler instance
    #' @param baseQuery                 base sql query
    #' @param countQuery                count query string (should match query). Can be auto generated with sub query
    #'                                  (default) but this will likely result in slow results
    #'
    #' @return self
    initialize = function(connectionHandler, baseQuery, countQuery = NULL) {
      checkmate::assertR6(connectionHandler, "ConnectionHandler")
      checkmate::assertString(baseQuery)
      checkmate::assertString(countQuery, null.ok = TRUE)
      # Cannot use multiple statments in a base query
      stopifnot(length(strsplit(baseQuery, ";")[[1]]) == 1)
      self$connectionHandler <- connectionHandler
      self$baseQuery <- baseQuery

      if (!is.null(countQuery)) {
        stopifnot(length(strsplit(countQuery, ";")[[1]]) == 1)
        self$countQuery <- countQuery
      } else {
        self$countQuery <-  sprintf("SELECT COUNT(*) as count FROM (\n%s\n) s;", self$baseQuery)
      }
    },

    #' get count
    #' @description
    #' execute count query with specified parameters
    #' @param ...
    #'
    #' @return count
    getCount = function(...) {
      sql <- SqlRender::render(sql = self$countQuery, ...)
      count <- self$connectionHandler$queryDb(sql)
      return(sum(count$count))
    },

    #' Get Page
    #'
    #' @param pageNum       page number
    #' @param pageSize      page size
    #' @param ...
    #'
    #' @return data.frame of query result
    getPage = function(pageNum, pageSize = self$pageSize, ...) {
      mainQuery <- SqlRender::render(sql = self$baseQuery, ...)
      pageOffset <- ((pageNum - 1) * pageSize)
      self$connectionHandler$queryDb("@main_query LIMIT @page_size OFFSET @page_offset",
                                     main_query = mainQuery,
                                     page_size = pageSize,
                                     page_offset = pageOffset)
    },

    #' get all results
    #'
    #' @param ...
    #'
    #' @return data.frame of all results. Used for large file downloads
    getAllResults = function(...) {
      self$connectionHandler$queryDb(self$baseQuery, ..., snakeCaseToCamelCase = FALSE)
    }
  )
)

#' Create Large Sql Query Data Table
#'
#' @description
#' Construct an instance of a LargeDataTable R6 instance for use inside largeTableServer
#'
#' This should pass a parameterized sql query that can be used to iteratively return data from a table
#' rather than returning the entire object.
#'
#' @param connectionHandler         ResultModelManager connectionHandler instance
#' @param connectionDetails         DatabaseConnector connectionDetails instance
#' @param baseQuery                 base sql query
#' @param countQuery                count query string (should match query). Can be auto generated with sub query
#'                                  (default) but this will likely result in slow results
#' @family LargeTables 
#' 
#' @export                                
createLargeSqlQueryDt <- function(connectionHandler = NULL,
                                  connectionDetails = NULL,
                                  baseQuery,
                                  countQuery = NULL) {
  if (is.null(connectionHandler)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
  }

  LargeDataTable$new(connectionHandler = connectionHandler,
                     baseQuery = baseQuery,
                     countQuery = countQuery)
}

#' Large Table Component Viewer
#' @description
#' Componenet for results sets with many thousands of rows
#' More limited than other table components in terms of automatic handling of search and
#' filtering but will allow responsive apps
#' @export
#'
#' @param id    Shiny module id. Must match largeTableServer
#' @param pageSizeChoices    numeric selection options for pages
#' @param selectedPageSize   numeric selection options for pages
#' @param fullDownloads     allow download button of full dataset from query
#' @family LargeTables
#' 
#' @export
largeTableView <- function(id, pageSizeChoices = c(10,25,50,100), selectedPageSize = 10, fullDownloads = TRUE) {
  ns <- shiny::NS(id)
  checkmate::assertNumeric(pageSizeChoices, min.len = 1, finite = TRUE, lower = 1)
  checkmate::assertTRUE(selectedPageSize %in% pageSizeChoices)

  inlineStyle <- "
  .inline-tv label{ display: table-cell; text-align: center; vertical-align: middle; padding-right: 1em; }
  .inline-tv .selectize-input { min-width:70px;}
  .inline-tv .form-group { display: table-row;}

  .pagination-buttons {
    margin-top: 1em;
  }

  .link-bt {
    background-color: transparent;
    border: none;
    border-radius: 3px;
    cursor: pointer;
    outline-style: solid;
    outline-width: 0;
    padding: 6px 12px;
    text-decoration:none;
   }
   .link-bt:hover{
      background-color: #eee;
   }
   .pagination-buttons a {
      text-decoration:none;
      color: #000!important;
   }"

  shiny::div(
    id = ns("display-table"),
    shiny::tags$head(
      shiny::tags$style(type = "text/css", inlineStyle)
    ),
    shiny::fluidRow(
      if (fullDownloads) {
        shiny::column(
          width = 4,
          shiny::downloadButton(ns("downloadFull"),
                                label = "Download",
                                icon = shiny::icon("download"))
        )
      }
    ),
    shinycssloaders::withSpinner(reactable::reactableOutput(ns("tableView"))),
    shiny::fluidRow(
      shiny::column(
        width = 4,
        class = "inline-tv",
        shiny::textOutput(ns("pageNumber")),
        shiny::selectInput(ns("pageSize"),
                           choices = pageSizeChoices,
                           selected = selectedPageSize,
                           label = "show",
                           width = "90px")
      ),
      shiny::column(
        width = 2
      ),
      shiny::column(
        width = 6,
        style = "text-align:right;",
        shiny::uiOutput(ns("pageActionButtons"))
      )
    )
  )
}

#' Large Table Component Server
#' @description
#' Display large data tables in a consistent way - server side pagination for reactable objects
#' @export
#' @param id            Shiny module id. Must match Large Table Viewer
#' @param ldt           LargeDataTable instance
#' @param inputParams   reactive that returns list of parameters to be passed to ldt
#' @param modifyData    optional callback function that takes the data page, page number, page size as parameters
#'                      must return data.frame compatable instance
#'
#' @param columns       List or reactable returning list of reactable::columnDef objects
#' @param ...           Additional reactable options (searchable, sortable
#' @family LargeTables
#' 
#' @export
largeTableServer <- function(id,
                             ldt,
                             inputParams,
                             modifyData = NULL,
                             columns = shiny::reactive(list()),
                             ...) {
  checkmate::assertR6(ldt, "LargeDataTable")

  if (!is.list(inputParams))
    checkmate::assertClass(inputParams, "reactive")

  if (!is.list(columns))
    checkmate::assertClass(columns, "reactive")

  shiny::moduleServer(id, function(input, output, session) {

    if (is.list(inputParams)) {
      realParams <- inputParams
      inputParams <- shiny::reactive(realParams)
    }

    if (is.list(columns)) {
      realColumns <- columns
      columns <- shiny::reactive(realColumns)
    }

    ns <- session$ns
    pageNum <- shiny::reactiveVal(1)
    pageSize <- shiny::reactive(as.integer(input$pageSize))

    rowCount <- shiny::reactive({
      do.call(ldt$getCount, inputParams())
    })

    pageCount <- shiny::reactive({
      CohortDiagnostics::safeMax(c(1, ceiling(rowCount()/pageSize())))
    })

    shiny::observeEvent(input$nextButton, pageNum(min(pageNum() + 1, rowCount())))
    shiny::observeEvent(input$previousButton, pageNum(CohortDiagnostics::safeMax(c(pageNum() - 1, 1))))

    shiny::observeEvent(input$pageNum, pageNum(input$pageNum))

    # Reset page on page size change or any input variable that could impact row count
    shiny::observe({
      inputParams()
      pageNum(1)
    })
    shiny::observeEvent(input$pageSize, { pageNum(1) })

    output$pageNumber <- shiny::renderText({

      rc <- format(rowCount(), big.mark = ",", scientific = FALSE)
      if (pageCount() < 2) {
        return(paste(rc, "rows"))
      }

      minNum <- format(((pageNum() - 1) * pageSize()) + 1, big.mark = ",", scientific = FALSE)
      maxNum <- min((pageNum() - 1)  * pageSize() + pageSize(), rowCount())
      maxNum <- format(maxNum, big.mark = ",", scientific = FALSE)
      return(paste(minNum, "-", maxNum, "of", rc, "rows"))
    })

    dataPage <- shiny::reactive({
      params <- inputParams()
      checkmate::assertList(params)
      params$pageNum <- pageNum()
      params$pageSize <- pageSize()

      dataPage <- do.call(ldt$getPage, params)

      if (is.function(modifyData)) {
        dataPage <- dataPage %>% modifyData(pageNum(), pageSize())
      }
      dataPage
    })

    output$tableView <- reactable::renderReactable({
      cols <- columns()
      checkmate::assertList(cols, null.ok = TRUE, types = "colDef")
      reactable::reactable(dataPage(),
                           columns = cols,
                           searchable = FALSE,
                           sortable = FALSE,
                           resizable = TRUE,
                           outlined = TRUE,
                           showSortIcon = TRUE,
                           striped = TRUE,
                           highlight = TRUE,
                           defaultColDef = reactable::colDef(align = "left"),
                           pagination = FALSE,
                           ...)
    })

    output$pageActionButtons <-  shiny::renderUI({
      pc <- pageCount()
      if (pc < 2) {
        return(shiny::span(style="width:80px;", shiny::HTML("&nbsp;")))
      }

      createPageLink <- function(pageLink) {
        js <- sprintf("Shiny.setInputValue('%s', %s);", ns("pageNum"), pageLink)
        shiny::tags$a(onclick = js,
                      class = "link-bt",
                      style = "cursor:pointer;", format(pageLink, big.mark = ",", scientific = FALSE))
      }

      linkNums <- unique(c(CohortDiagnostics::safeMax(c(2, pageNum() - 2)):min(pageCount() - 1, pageNum() + 3)))
      links <- lapply(linkNums, createPageLink)

      ## render action buttons
      # Always show 1
      # Show row up to 5
      # Always show max
      shiny::div(
        class = "pagination-buttons",
        shiny::actionLink(ns("previousButton"), label = "Previous", class = "link-bt"),
        createPageLink(1),
        if (!pageNum() %in% c(1, 2)) {
          shiny::span("...", class = "link-bt")
        },
        links,
        if (pageCount() != pageNum()) {
          shiny::span("...", class = "link-bt")
        },
        createPageLink(pageCount()),
        shiny::actionLink(ns("nextButton"), label = "Next", class = "link-bt")
      )
    })

    output$downloadFull <- shiny::downloadHandler(
      filename = function() {
        paste0('result-data-full-', id, Sys.Date(), '.csv')
      },
      content = function(con) {
        shiny::withProgress(
          message = "preparing download",
          value = 15,
          readr::write_csv(do.call(ldt$getAllResults, inputParams()), con)
        )
      }
    )
  })
}
