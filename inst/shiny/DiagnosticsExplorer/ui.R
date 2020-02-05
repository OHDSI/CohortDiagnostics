library(shiny)
library(shinydashboard)
library(DT)

dashboardPage(
  dashboardHeader(title = "Cohort Diagnostics Explorer"),
  dashboardSidebar(
    sidebarMenu(id = "tabs",
      menuItem("Incidence Proportion", tabName = "incidenceProportion"),
      menuItem("Time Distributions", tabName = "timeDistribution"),
      menuItem("Included (Source) Concepts", tabName = "includedConcepts"),
      menuItem("Orphan (Source) Concepts", tabName = "orphanConcepts"),
      menuItem("Inclusion Rule Statistics", tabName = "inclusionRuleStats"),
      menuItem("Index Event Breakdown", tabName = "indexEventBreakdown"),
      menuItem("Cohort Characterization", tabName = "cohortCharacterization"),
      menuItem("Cohort Overlap", tabName = "cohortOverlap"),
      menuItem("Compare Cohort Characterization", tabName = "compareCohortCharacterization"),
      conditionalPanel(condition="input.tabs!='incidenceProportion' & input.tabs!='timeDistribution'",
                       selectInput("database", "Database", database$databaseId, selectize = FALSE)
      ),
      conditionalPanel(condition="input.tabs=='incidenceProportion' | input.tabs=='timeDistribution'",
                       checkboxGroupInput("databases", "Database", database$databaseId, selected = database$databaseId[1])
      ),
      selectInput("cohort", "Cohort", choices = cohort$cohortName, selectize = FALSE),
      conditionalPanel(condition="input.tabs=='includedConcepts' | input.tabs=='orphanConcepts'",
                       selectInput("conceptSet", "Concept Set", c(""), selectize = FALSE)
      ),
      conditionalPanel(condition="input.tabs=='cohortOverlap' | input.tabs=='compareCohortCharacterization'",
                       selectInput("comparator", "Comparator", cohort$cohortName, selectize = FALSE)
      )
    )
  ),
  dashboardBody(
    tabItems(
      tabItem(tabName = "incidenceProportion",
              box(
                title = "Incidence Proportion", width = NULL, status = "primary",
                plotOutput("incidenceProportionPlot")
              )
      ),
      tabItem(tabName = "timeDistribution",
              box(
                title = "Time Distributions", width = NULL, status = "primary",
                plotOutput("timeDisPlot")
              ),
              box(
                title = "Time Distributions Table", width = NULL, status = "primary",
                dataTableOutput("timeDistTable")
              )
      ),
      tabItem(tabName = "includedConcepts",
              radioButtons("includedType", "", c("Source Concepts", "Standard Concepts"), selected = "Source Concepts", inline = TRUE),
              dataTableOutput("includedConceptsTable")
      ),
      tabItem(tabName = "orphanConcepts",
              dataTableOutput("orphanConceptsTable")
      ),
      tabItem(tabName = "inclusionRuleStats",
              dataTableOutput("inclusionRuleTable")
      ),
      tabItem(tabName = "indexEventBreakdown",
              dataTableOutput("breakdownTable")
      ),
      tabItem(tabName = "cohortCharacterization",
              radioButtons("charType", "", c("Pretty", "Raw"), selected = "Pretty", inline = TRUE),
              dataTableOutput("characterizationTable")
      ),
      tabItem(tabName = "cohortOverlap",
              box(
                title = "Cohort Overlap (Subjects)", width = NULL, status = "primary",
                plotOutput("overlapPlot")
              ),
              box(
                title = "Cohort Overlap Statistics", width = NULL, status = "primary",
                dataTableOutput("overlapTable")
              )
      ),
      tabItem(tabName = "compareCohortCharacterization",
              radioButtons("charCompareType", "", c("Pretty", "Raw"), selected = "Pretty", inline = TRUE),
              dataTableOutput("charCompareTable")
      )
    )
  )
)