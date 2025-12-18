test_that("retry reader returns a data.frame for a tiny slice", {
  skip_on_cran()
  con <- tnp_duckdb(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  man <- tnp_manifest()
  res <- tnp_lake_read_https(
    con, man, state = "NY", initials = "S",
    columns = c("npi","first_name","last_name","state"),
    max_urls_per_query = 1, tries = 3, initial_wait = 0.6,
    max_wait = 3, sleep_between_batches = 0.2, verbose = FALSE
  )
  expect_s3_class(res, "data.frame")
  expect_true(all(c("npi","first_name","last_name","state") %in% names(res)))
})
