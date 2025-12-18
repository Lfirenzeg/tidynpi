#' Build parquet URLs for a (state, initials) selection
#'
#' @param man Manifest list returned by [tnp_manifest()].
#' @param state Two-letter state code (e.g., "NY").
#' @param initials NULL for all initials, or a character vector
#'   like c("S","M","%23","_","1").
#' @return Character vector of HTTPS URLs to parquet shards.
#' @export
tnp_urls <- function(man, state, initials = NULL) {
  st <- man$states[[state]]
  if (is.null(initials)) initials <- names(st)
  initials <- intersect(initials, names(st))
  unlist(lapply(st[initials], `[[`, "files"), use.names = FALSE)
}
