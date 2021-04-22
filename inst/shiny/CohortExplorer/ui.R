shinyUI(fluidPage(titlePanel(
  sprintf(
    "Exploring cohort %s in %s.%s",
    cohortDefinitionId,
    cohortDatabaseSchema,
    cohortTable
  )
),
fluidRow(
  column(
    2,
    tags$label(class = "control-label", `for` = "subjectId", "Subject ID"),
    textOutput("subjectId"),
    tags$label(class = "control-label", `for` = "age", "Age at First Index Date"),
    textOutput("age"),
    tags$label(class = "control-label", `for` = "gender", "Gender"),
    textOutput("gender"),
    actionButton("previousButton", "<"),
    actionButton("nextButton", ">"),
    checkboxGroupInput(
      "domains",
      label = "Domains",
      choices = c(
        "Condition era",
        "Condition occurrence",
        "Drug era",
        "Drug exposure",
        "Procedure",
        "Measurement",
        "Observation",
        "Visit",
        "Observation period"
      )
    ),
    textAreaInput(
      "filterRegex",
      div("Concept Name Filter", actionLink("filterInfo", "", icon = icon("info-circle"))),
      placeholder = "regex"
    ),
    checkboxInput("showPlot", "Show Plot", value = TRUE),
    checkboxInput("showTable", "Show Table", value = TRUE),
  ),
  column(
    10,
    conditionalPanel(
      condition = "input.showTable==1",
      conditionalPanel(condition = "input.showPlot==1",
                       plotlyOutput("plotSmall", height = "400px")),
      dataTableOutput("eventTable")
    ),
    conditionalPanel(condition = "input.showTable==0 & input.showPlot==1",
                     plotlyOutput("plotBig", height = "800px"),)
  )
)))
