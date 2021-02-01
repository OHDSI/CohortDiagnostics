# demoing group support in the `choices` arg
shinyApp(
    ui = fluidPage(
        shiny::selectizeInput(inputId = "state", label = NULL,
                    choices = 
                    list(`East Coast` = list("NY", "NJ", "CT"),
                         `West Coast` = list("WA", "OR", "CA"),
                         `Midwest` = list("MN", "WI", "IA"))
        ),
        textOutput("result")
    ),
    server = function(input, output) {
        shiny::updateSelectizeInput(session = session, inputId = "state", label = "Choose a state")
        output$result <- renderText({
            paste("You chose", input$state)
        })
    }
)
