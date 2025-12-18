#' Read from the public HTTPS lake with batching + retries (rate-limit friendly)
#'
#' @param con DuckDB connection (from `tnp_duckdb()`).
#' @param man Manifest list (from `tnp_manifest()`).
#' @param state Two-letter state (example, "NY").
#' @param initials Optional vector of initials to include (example, c("S","M")); NULL = all.
#' @param where_sql Optional SQL predicate to push down (example, "entity_type = 1").
#' @param columns Character vector of columns to select.
#' @param max_urls_per_query How many Parquet files to read per SQL call.
#' @param tries Max retries per query if rate limited.
#' @param initial_wait First backoff wait.
#' @param max_wait Max backoff wait.
#' @param sleep_between_batches Pause between batches to be gentle.
#' @param verbose Print progress / retry messages.
#'
#' @return A data.frame with the bound results (0 rows if no matches).
#' @export
tnp_lake_read_https <- function(
    con, man, state, initials = NULL,
    where_sql = NULL,
    columns = c("npi", "first_name", "last_name", "state"),
    max_urls_per_query = 2,
    tries = 6,
    initial_wait = 0.8,
    max_wait = 8,
    sleep_between_batches = 0.3,
    verbose = interactive()
) {
  # Get the HTTPS URLs for the selection
  urls <- tnp_urls(man, state, initials)
  if (length(urls) == 0) {
    if (verbose) cli::cli_alert_info("No URLs for state {state} and given initials.")
    return(utils::head(data.frame(), 0))
  }

  # Build the SELECT template (we fill the parquet_scan([...]) per batch)
  sel <- paste(columns, collapse = ", ")
  where_clause <- if (is.null(where_sql)) "" else paste(" WHERE ", where_sql)

  # Batch the files to avoid 429s
  chunks <- tnp_split_every(urls, max_urls_per_query)
  out <- vector("list", length(chunks))
  total <- length(chunks)

  if (verbose) cli::cli_alert_info(
    "Reading {length(urls)} files in {total} batch(es) (max {max_urls_per_query} per query)."
  )

  for (i in seq_along(chunks)) {
    u <- chunks[[i]]
    urls_sql <- paste(sprintf("'%s'", u), collapse = ", ")
    sql <- sprintf("SELECT %s FROM parquet_scan([%s])%s", sel, urls_sql, where_clause)

    if (verbose) cli::cli_alert("Batch {i}/{total} ({length(u)} file(s))")
    out[[i]] <- tnp_retry_query(
      con, sql, tries = tries, initial_wait = initial_wait,
      max_wait = max_wait, verbose = verbose
    )

    if (i < total && sleep_between_batches > 0) Sys.sleep(sleep_between_batches)
  }

  dplyr::bind_rows(out)
}
