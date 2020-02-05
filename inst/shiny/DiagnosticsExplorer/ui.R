library(shiny)
library(shinydashboard)
library(DT)

dashboardPage(
  dashboardHeader(title = "Cohort Diagnostics Explorer"),
  dashboardSidebar(
    sidebarMenu(id = "tabs",
      menuItem("Incidence Proportion", tabName = "incidenceProportion"),
      menuItem("Included (Source) Concepts", tabName = "includedConcepts"),
      menuItem("Orphan (Source) Concepts", tabName = "orphanConcepts"),
      menuItem("Inclusion Rule Statistics", tabName = "inclusionRuleStats"),
      menuItem("Index Event Breakdown", tabName = "indexEventBreakdown"),
      menuItem("Cohort Characterization", tabName = "cohortCharacterization"),
      menuItem("Cohort Overlap", tabName = "cohortOverlap"),
      menuItem("Compare Cohort Characterization", tabName = "compareCohortCharacterization"),
      conditionalPanel(condition="input.tabs!='incidenceProportion'",
                       selectInput("database", "Database", database$databaseId)
      ),
      conditionalPanel(condition="input.tabs=='incidenceProportion'",
                       checkboxGroupInput("databases", "Database", database$databaseId, selected = database$databaseId[1])
      ),
      selectInput("cohort", "Cohort", cohort$cohortName),
      conditionalPanel(condition="input.tabs=='includedConcepts' | input.tabs=='orphanConcepts'",
                       selectInput("conceptSet", "Concept Set", c(""))
      ),
      conditionalPanel(condition="input.tabs=='cohortOverlap' | input.tabs=='compareCohortCharacterization'",
                       selectInput("comparator", "Comparator", cohort$cohortName)
      )
    )
  ),
  dashboardBody(
    tabItems(
      tabItem(tabName = "incidenceProportion",
              plotOutput("incidenceProportionPlot")
      ),
      tabItem(tabName = "includedConcepts",
              dataTableOutput("includedSourceConceptsTable")
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
              plotOutput("overlapPlot"),
              uiOutput("overlapUi")
      ),
      tabItem(tabName = "compareCohortCharacterization",
              radioButtons("charCompareType", "", c("Pretty", "Raw"), selected = "Pretty", inline = TRUE),
              dataTableOutput("charCompareTable")
      )
    )
  )
)