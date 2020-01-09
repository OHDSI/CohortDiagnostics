library(shiny)
library(DT)

shinyUI(
  fluidPage(style = "width:1500px;",
            titlePanel("Diagnostics Explorer"),
            tags$head(tags$style(type = "text/css", "
             #loadmessage {
                                 position: fixed;
                                 top: 0px;
                                 left: 0px;
                                 width: 100%;
                                 padding: 5px 0px 5px 0px;
                                 text-align: center;
                                 font-weight: bold;
                                 font-size: 100%;
                                 color: #000000;
                                 background-color: #ADD8E6;
                                 z-index: 105;
                                 }
                                 ")),
            conditionalPanel(condition = "$('html').hasClass('shiny-busy')",
                             tags$div("Procesing...",id = "loadmessage")),
            fluidRow(
              column(3,
                     selectInput("database", "Database", database$databaseId),
                     selectInput("cohort", "Cohort", cohort$cohortName),
                     conditionalPanel(condition="input.detailsTabsetPanel==7 | input.detailsTabsetPanel==8",
                                      selectInput("comparator", "Comparator", cohort$cohortName)
                     ),
                     conditionalPanel(condition="input.detailsTabsetPanel==2 | input.detailsTabsetPanel==3",
                                      selectInput("conceptSet", "Concept Set", c(""))
                     )
              ),
              column(9,
                     tabsetPanel(id = "detailsTabsetPanel",
                                 tabPanel("Incidence proportion",
                                          value = 1,
                                          plotOutput("incidenceProportionPlot")
                                 ),
                                 tabPanel("Included source concepts",
                                          value = 2,
                                          dataTableOutput("includedSourceConceptsTable")
                                 ),
                                 tabPanel("Orphan (source) concepts",
                                          value = 3,
                                          dataTableOutput("orphanConceptsTable")
                                 ),
                                 tabPanel("Inclusion rule stats",
                                          value = 4,
                                          dataTableOutput("inclusionRuleTable")
                                 ),
                                 tabPanel("Index event breakdown",
                                          value = 5,
                                          dataTableOutput("breakdownTable")
                                 ),
                                 tabPanel("Cohort characterization",
                                          value = 6,
                                          radioButtons("charType", "", c("Pretty", "Raw"), selected = "Pretty", inline = TRUE),
                                          dataTableOutput("characterizationTable")
                                 ),
                                 tabPanel("Cohort overlap",
                                          value = 7,
                                          uiOutput("overlapUi")
                                 ),
                                 tabPanel("Compare cohort characterization",
                                          value = 8,
                                          radioButtons("charCompareType", "", c("Pretty", "Raw"), selected = "Pretty", inline = TRUE),
                                          dataTableOutput("charCompareTable")
                                 )
                                 
                     )
              )
            )
            
  )
)
