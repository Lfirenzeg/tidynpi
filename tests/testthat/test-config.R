test_that("default URLs are strings", {
  expect_type(tnp_manifest_url(), "character")
  expect_true(grepl("^https?://", tnp_manifest_url()))
})
