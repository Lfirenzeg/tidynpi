test_that("URL picker returns vectors of URLs", {
  man <- tnp_manifest()
  urls <- tnp_urls(man, "NY", initials = "S")
  expect_true(is.character(urls))
  expect_gt(length(urls), 0)
  expect_true(grepl("\\.parquet$", urls[1]))
})
