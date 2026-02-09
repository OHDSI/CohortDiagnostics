test_that("multiplication works", {
  outdir <- getUniqueTempDir()
  dir.create(outdir, recursive = TRUE)
  tempLog <- file.path(outdir, "tempLog.txt")
  on.exit(unlink(tempLog))
  writeLines(
    text = c(
      "2024-10-09 10:25:03	[Main thread]	INFO	CohortDiagnostics	makeDataExportable	 - Unexpected fields found in table concept_sets - databaseId. These fields will be ignored.",
      "2024-10-09 10:25:03	[Main thread]	DEBUG	CohortDiagnostics	writeToCsv.default	creating results/concept_sets.csv",
      "2024-10-09 10:25:03	[Main thread]	INFO	CohortDiagnostics	runConceptSetDiagnostics	Starting concept set diagnostics",
      "2024-10-09 10:25:03	[Main thread]	INFO	CohortDiagnostics	instantiateUniqueConceptSets	Instantiating concept sets",
      "2024-10-09 10:25:05	[Main thread]	INFO	CohortDiagnostics	createConceptCountsTable	Creating internal concept counts table",
      "2024-10-09 10:34:17	[Main thread]	INFO	DatabaseConnector	executeSql	Executing SQL took 9.2 mins",
      "2024-10-09 10:34:17	[Main thread]	INFO	CohortDiagnostics	timeExecution	Fetching included source concepts",
      "2024-10-09 10:40:03	[Main thread]	INFO	DatabaseConnector	executeSql	Executing SQL took 5.76 mins",
      "2024-10-09 10:40:03	[Main thread]	TRACE	CohortDiagnostics	makeDataExportable	 - Ensuring data is exportable: included_source_concept",
      "2024-10-09 10:40:03	[Main thread]	TRACE	CohortDiagnostics	makeDataExportable	  - Found in table included_source_concept the following fields: databaseId, cohortId, conceptSetId, conceptId, sourceConceptId, conceptCount, conceptSubjects",
      "2024-10-09 10:40:03	[Main thread]	INFO	CohortDiagnostics	enforceMinCellValue	- Censoring 1455 values (21.8%) from conceptSubjects because value below minimum",
      "2024-10-09 10:40:03	[Main thread]	INFO	CohortDiagnostics	enforceMinCellValue	- Censoring 883 values (13.2%) from conceptCount because value below minimum",
      "2024-10-09 10:40:03	[Main thread]	DEBUG	CohortDiagnostics	writeToCsv.default	creating results/included_source_concept.csv",
      "2024-10-09 10:40:04	[Main thread]	INFO	CohortDiagnostics	timeExecution	Finding source codes took 5.78 mins",
      "2024-10-09 10:40:04	[Main thread]	INFO	CohortDiagnostics	runConceptSetDiagnostics	Breaking down index events",
      "2024-10-09 10:40:05	[Main thread]	INFO	CohortDiagnostics	FUN	- Breaking down index events for cohort 'Hypertension'",
      "2024-10-09 10:40:25	[Main thread]	INFO	CohortDiagnostics	FUN	- Breaking down index events for cohort 'Acetaminophen'",
      "2024-10-09 10:41:25	[Main thread]	INFO	CohortDiagnostics	FUN	- Breaking down index events for cohort 'Amlodipine'",
      "2024-10-09 10:42:20	[Main thread]	INFO	CohortDiagnostics	FUN	- Breaking down index events for cohort 'Hydrochlorothiazide'",
      "2024-10-09 10:43:19	[Main thread]	INFO	CohortDiagnostics	FUN	- Breaking down index events for cohort 'Lisinopril'",
      "2024-10-09 10:44:15	[Main thread]	INFO	CohortDiagnostics	FUN	- Breaking down index events for cohort 'Losartan'",
      "2024-10-09 10:45:10	[Main thread]	INFO	CohortDiagnostics	FUN	- Breaking down index events for cohort 'Metorolol'",
      "2024-10-09 10:46:12	[Main thread]	INFO	CohortDiagnostics	enforceMinCellValue	- Censoring 794 values (24.6%) from conceptCount because value below minimum",
      "2024-10-09 10:46:12	[Main thread]	INFO	CohortDiagnostics	enforceMinCellValue	- Censoring 803 values (24.8%) from subjectCount because value below minimum",
      "2024-10-09 10:46:12	[Main thread]	TRACE	CohortDiagnostics	makeDataExportable	 - Ensuring data is exportable: index_event_breakdown",
      "2024-10-09 10:46:12	[Main thread]	TRACE	CohortDiagnostics	makeDataExportable	  - Found in table index_event_breakdown the following fields: domainTable, domainField, conceptId, conceptCount, subjectCount, cohortId, databaseId",
      "2024-10-09 10:46:12	[Main thread]	INFO	CohortDiagnostics	enforceMinCellValue	- Censoring 0 values (0%) from conceptCount because value below minimum",
      "2024-10-09 10:46:12	[Main thread]	INFO	CohortDiagnostics	enforceMinCellValue	- Censoring 0 values (0%) from subjectCount because value below minimum",
      "2024-10-09 10:46:12	[Main thread]	DEBUG	CohortDiagnostics	writeToCsv.default	creating results/index_event_breakdown.csv",
      "2024-10-09 10:46:13	[Main thread]	INFO	CohortDiagnostics	runConceptSetDiagnostics	Breaking down index event took 6.15 mins",
      "2024-10-09 10:46:13	[Main thread]	INFO	CohortDiagnostics	runConceptSetDiagnostics	Finding orphan concepts",
      "2024-10-09 10:46:13	[Main thread]	TRACE	CohortDiagnostics	runConceptSetDiagnostics	Using internal concept count table.",
      "2024-10-09 10:46:13	[Main thread]	INFO	CohortDiagnostics	runConceptSetDiagnostics	- Finding orphan concepts for concept set '[AM] Hypertension diagnosis'",
      "2024-10-09 10:46:21	[Main thread]	INFO	DatabaseConnector	executeSql	Executing SQL took 8.36 secs",
      "2024-10-09 10:46:21	[Main thread]	TRACE	CohortDiagnostics	.findOrphanConcepts	- Fetching orphan concepts from server",
      "2024-10-09 10:46:21	[Main thread]	TRACE	CohortDiagnostics	.findOrphanConcepts	- Dropping orphan temp tables",
      "2024-10-09 10:46:22	[Main thread]	INFO	CohortDiagnostics	runConceptSetDiagnostics	- Finding orphan concepts for concept set '[AM] Hypertension drugs'"
    ),
    con = tempLog
  )
  g <- plotLogFile(tempLog)

  expect_s3_class(object = g, class = c("gg", "ggplot"))
})
