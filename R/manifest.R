#' Fetch the NPPES manifest (JSON)
#' @param use_latest logical; if TRUE, resolve latest.txt first.
#' @return a named list with $snapshot, $base_url, $states
#' @export
tnp_manifest <- function(use_latest = FALSE) {
  man_url <- if (use_latest) {
    latest <- httr2::request(tnp_latest_url()) |> httr2::req_perform() |> httr2::resp_body_string()
    paste0(dirname(tnp_latest_url()), "/", trimws(latest), "/manifest_state_files.json")
  } else tnp_manifest_url()

  resp <- httr2::request(man_url) |> httr2::req_perform()
  jsonlite::fromJSON(httr2::resp_body_string(resp), simplifyVector = FALSE)
}

#' List available initials for a state
#'
#' @param man Manifest list returned by [tnp_manifest()].
#' @param state Two-letter state code (for example "NY").
#' @return A character vector of initials present in the lake for that state.
#' @export
tnp_state_initials <- function(man, state) {
  stopifnot(state %in% names(man$states))
  names(man$states[[state]])
}
