# Default public endpoints (rnppes.org)
.tnp_default_manifest <- "https://rnppes.org/nppes/2025-10-13/manifest_state_files.json"
.tnp_latest_url       <- "https://rnppes.org/nppes/latest.txt"

tnp_manifest_url <- function() {
  Sys.getenv("TNP_MANIFEST_URL", unset = .tnp_default_manifest)
}
tnp_latest_url <- function() {
  Sys.getenv("TNP_LATEST_URL", unset = .tnp_latest_url)
}
