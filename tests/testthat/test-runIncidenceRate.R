
# test getIncidenceRate on all dbms
for (nm in names(testServers)) {
  server <- testServers[[nm]]
  
  test_that(paste("getIncidenceRate works on", nm), {
    
    # add tests. check not only that the function runs but gives the correct result
    
  })
}



# test runIncidenceRate on eunomia

test_that("runIncidenceRate", {
  server <- testServers[[nm]]
  exportFolder <- tempfile()
  dir.create(exportFolder)
  
  incrementalFolder <- tempfile()
  dir.create(incrementalFolder)
  
  # add tests
  
})



