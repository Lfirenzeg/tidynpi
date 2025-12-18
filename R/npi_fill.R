#' Fill basic provider fields from NPI using the lake
#'
#' This is a convenience lookup: given one or more NPIs, return name + state (+ city/tax codes if present).
#' For speed, you can pass `states` to restrict where to search.
#'
#' @param npi Vector of NPIs (numeric or character).
#' @param con Optional DuckDB connection. If NULL, tnp_duckdb() is used.
#' @param man Optional manifest. If NULL, tnp_manifest() is used.
#' @param states Optional vector of 2-letter state codes to restrict scanning.
#' @param max_urls_per_query URL batching (rate-limit mitigation).
#' @param ... Passed to tnp_lake_read_https().
#' @return A data.frame of matched rows.
#' @export
npi_fill <- function(
    npi,
    con = NULL,
    man = NULL,
    states = NULL,
    max_urls_per_query = 3,
    ...
) {
  if (is.null(con)) {
    con <- tnp_duckdb()
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  }
  if (is.null(man)) man <- tnp_manifest()

  npi <- as.character(npi)
  npi <- trimws(npi)
  npi <- npi[npi != ""]
  if (!length(npi)) return(data.frame())

  # Limit states if provided
  if (is.null(states)) {
    states <- names(man$states)
  } else {
    states <- toupper(trimws(states))
    states <- intersect(states, names(man$states))
  }

  found <- list()
  remaining <- unique(npi)

  for (st in states) {
    if (!length(remaining)) break

    # All initials for that state
    urls <- tnp_urls(man, st, initials = NULL)
    if (!length(urls)) next

    # Detect cols (v2 vs v3)
    cols_avail <- tnp_lake_columns(con, urls[1])
    cols <- intersect(
      c("npi","first_name","last_name_leg","state","city",
        "tax_code_1","tax_code_2","tax_code_3","tax_code_4","tax_code_5"),
      cols_avail
    )

    # WHERE npi IN (...)
    # Keep it safe and not too huge per state
    chunk <- remaining
    if (length(chunk) > 5000) chunk <- chunk[1:5000]

    in_sql <- paste(sprintf("%s", chunk), collapse = ",")
    where_sql <- sprintf("npi IN (%s)", in_sql)

    res <- tnp_lake_read_https(
      con, man,
      state = st,
      initials = NULL,
      columns = cols,
      where_sql = where_sql,
      max_urls_per_query = max_urls_per_query,
      ...
    )

    if (nrow(res)) {
      found[[st]] <- res
      remaining <- setdiff(remaining, as.character(res$npi))
    }
  }

  dplyr::bind_rows(found)
}
