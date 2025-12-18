test_that("manifest loads and has expected shape", {
  man <- tnp_manifest(use_latest = FALSE)
  expect_true(is.list(man))
  expect_true(all(c("snapshot","base_url","states") %in% names(man)))
  expect_true("NY" %in% names(man$states))
})
