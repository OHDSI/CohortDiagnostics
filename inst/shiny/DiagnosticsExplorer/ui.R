library(shiny)
library(shinydashboard)
library(DT)

dashboardPage(
  dashboardHeader(title = "Cohort Diagnostics Explorer"),
  dashboardSidebar(
    sidebarMenu(id = "tabs",
      if (exists("cohortCount")) menuItem("Cohort Counts", tabName = "cohortCounts"),
      if (exists("incidenceProportion")) menuItem("Incidence Proportion", tabName = "incidenceProportion"),
      if (exists("timeDistribution")) menuItem("Time Distributions", tabName = "timeDistribution"),
      if (exists("includedSourceConcept")) menuItem("Included (Source) Concepts", tabName = "includedConcepts"),
      if (exists("orphanConcept")) menuItem("Orphan (Source) Concepts", tabName = "orphanConcepts"),
      if (exists("inclusionRuleStats")) menuItem("Inclusion Rule Statistics", tabName = "inclusionRuleStats"),
      if (exists("indexEventBreakdown")) menuItem("Index Event Breakdown", tabName = "indexEventBreakdown"),
      if (exists("covariateValue")) menuItem("Cohort Characterization", tabName = "cohortCharacterization"),
      if (exists("cohortOverlap")) menuItem("Cohort Overlap", tabName = "cohortOverlap"),
      if (exists("covariateValue")) menuItem("Compare Cohort Characterization", tabName = "compareCohortCharacterization"),
      conditionalPanel(condition = "input.tabs!='incidenceProportion' & input.tabs!='timeDistribution' & input.tabs!='cohortCharacterization' & input.tabs!='cohortCounts'",
                       selectInput("database", "Database", database$databaseId, selectize = FALSE)
      ),
      conditionalPanel(condition = "input.tabs=='incidenceProportion' | input.tabs=='timeDistribution' | input.tabs=='cohortCharacterization' | input.tabs=='cohortCounts'",
                       checkboxGroupInput("databases", "Database", database$databaseId, selected = database$databaseId[1])
      ),
      conditionalPanel(condition = "input.tabs!='cohortCounts'",
                       selectInput("cohort", "Cohort", choices = cohort$cohortFullName, selectize = FALSE)
      ),
      conditionalPanel(condition = "input.tabs=='includedConcepts' | input.tabs=='orphanConcepts'",
                       selectInput("conceptSet", "Concept Set", c(""), selectize = FALSE)
      ),
      conditionalPanel(condition = "input.tabs=='cohortOverlap' | input.tabs=='compareCohortCharacterization'",
                       selectInput("comparator", "Comparator", cohort$cohortFullName, selectize = FALSE)
      )
    )
  ),
  dashboardBody(
    tabItems(
      tabItem(tabName = "cohortCounts",
              dataTableOutput("cohortCountsTable")
      ),
      tabItem(tabName = "incidenceProportion",
              box(
                title = "Incidence Proportion", width = NULL, status = "primary",
                plotOutput("incidenceProportionPlot", height = 800)
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