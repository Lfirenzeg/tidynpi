#' Retry a DuckDB SQL query with exponential backoff (handles HTTP 429)
#'
#' @param con DBI connection (DuckDB).
#' @param sql SQL string to execute.
#' @param tries Maximum attempts.
#' @param initial_wait Seconds to wait before the first retry.
#' @param max_wait Cap for backoff sleep between retries.
#' @param verbose Whether to print retry messages.
#' @return A data.frame (result of the query) or error if all retries fail.
#' @keywords internal
tnp_retry_query <- function(con, sql, tries = 6, initial_wait = 0.8, max_wait = 8,
                            verbose = interactive()) {
  wait <- initial_wait
  last_msg <- NULL

  for (i in seq_len(tries)) {
    res <- try(DBI::dbGetQuery(con, sql), silent = TRUE)
    if (!inherits(res, "try-error")) return(res)

    # Extract error message
    last_msg <- conditionMessage(attr(res, "condition"))
    # Heuristics for public dev URL throttling / transient network
    is_429 <- grepl("HTTP 429|Too Many Requests", last_msg, ignore.case = TRUE)
    is_rate <- grepl("rate limit", last_msg, ignore.case = TRUE)
    is_net  <- grepl("HTTP GET error|timed out|Temporary failure|EOF",
                     last_msg, ignore.case = TRUE)

    if (is_429 || is_rate || is_net) {
      if (verbose) cli::cli_alert_warning(
        "Query failed (attempt {i}/{tries}): {last_msg}\n  waiting {round(wait,2)}s before retry..."
      )
      # jitter to avoid herding
      sleep_for <- wait + stats::runif(1, min = 0, max = wait * 0.15)
      Sys.sleep(sleep_for)
      wait <- min(max_wait, wait * 1.8)
      next
    }

    # Non-retryable error
    stop(res)
  }

  stop("Query failed after ", tries, " attempts. Last error: ", last_msg)
}

#' Split a vector into chunks of ~n elements
#' @keywords internal
tnp_split_every <- function(x, n) {
  if (length(x) == 0) return(list())
  idx <- ceiling(seq_along(x) / n)
  split(x, idx)
}
