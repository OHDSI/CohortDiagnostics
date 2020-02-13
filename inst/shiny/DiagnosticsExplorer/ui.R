library(shiny)
library(shinydashboard)
library(DT)

addInfo <- function(item, infoId) {
  infoTag <- tags$small(class = "badge pull-right action-button",
                        style = "padding: 1px 6px 2px 6px; background-color: steelblue;",
                        type = "button", 
                        id = infoId,
                        "i")
  item$children[[1]]$children <- append(item$children[[1]]$children, list(infoTag))
  return(item)
}

dashboardPage(
  dashboardHeader(title = "Cohort Diagnostics"),
  dashboardSidebar(
    sidebarMenu(id = "tabs",
      if (exists("cohortCount")) addInfo(menuItem("Cohort Counts", tabName = "cohortCounts"), "cohortCountsInfo"),
      if (exists("incidenceRate")) addInfo(menuItem("Incidence Rate", tabName = "incidenceRate"), "incidenceRateInfo"),
      if (exists("timeDistribution")) addInfo(menuItem("Time Distributions", tabName = "timeDistribution"), "timeDistributionInfo"),
      if (exists("includedSourceConcept")) addInfo(menuItem("Included (Source) Concepts", tabName = "includedConcepts"), "includedConceptsInfo"),
      if (exists("orphanConcept")) addInfo(menuItem("Orphan (Source) Concepts", tabName = "orphanConcepts"), "orphanConceptsInfo"),
      if (exists("inclusionRuleStats")) addInfo(menuItem("Inclusion Rule Statistics", tabName = "inclusionRuleStats"), "inclusionRuleStatsInfo"),
      if (exists("indexEventBreakdown")) addInfo(menuItem("Index Event Breakdown", tabName = "indexEventBreakdown"), "indexEventBreakdownInfo"),
      if (exists("covariateValue")) addInfo(menuItem("Cohort Characterization", tabName = "cohortCharacterization"), "cohortCharacterizationInfo"),
      if (exists("cohortOverlap")) addInfo(menuItem("Cohort Overlap", tabName = "cohortOverlap"), "cohortOverlapInfo"),
      if (exists("covariateValue")) addInfo(menuItem("Compare Cohort Char.", tabName = "compareCohortCharacterization"), "compareCohortCharacterizationInfo"),
      conditionalPanel(condition = "input.tabs!='incidenceRate' & input.tabs!='timeDistribution' & input.tabs!='cohortCharacterization' & input.tabs!='cohortCounts'",
                       selectInput("database", "Database", database$databaseId, selectize = FALSE)
      ),
      conditionalPanel(condition = "input.tabs=='incidenceRate' | input.tabs=='timeDistribution' | input.tabs=='cohortCharacterization' | input.tabs=='cohortCounts'",
                       checkboxGroupInput("databases", "Database", database$databaseId, selected = database$databaseId[1])
      ),
      conditionalPanel(condition = "input.tabs!='cohortCounts'",
                       selectInput("cohort", "Cohort", choices = cohort$cohortFullName, selectize = FALSE)
      ),
      conditionalPanel(condition = "input.tabs=='includedConcepts' | input.tabs=='orphanConcepts'",
                       selectInput("conceptSet", "Concept Set", c(""), selectize = FALSE)
      ),
      conditionalPanel(condition = "input.tabs=='cohortOverlap' | input.tabs=='compareCohortCharacterization'",
                       selectInput("comparator", "Comparator", cohort$cohortFullName, selectize = FALSE, selected = cohort$cohortFullName[min(2, nrow(cohort))])
      )
    )
  ),
  dashboardBody(
    tabItems(
      tabItem(tabName = "cohortCounts",
              dataTableOutput("cohortCountsTable")
      ),
      tabItem(tabName = "incidenceRate",
              box(
                title = "Incidence Rate", width = NULL, status = "primary",
                checkboxGroupInput(inputId = "irStratification", 
                                   label = "Stratify by", 
                                   choices = c("Age", "Gender", "Calendar Year"), 
                                   selected = c("Age", "Gender", "Calendar Year"),
                                   inline = TRUE),
                # checkboxInput("completePanelsOnly", "Complete panels only", value = TRUE),
                plotOutput("incidenceRatePlot", height = 800)
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