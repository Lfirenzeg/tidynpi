#' Read a slice from the lake
#' @param con duckdb connection
#' @param man manifest list
#' @param state 2-letter code (for example "NY")
#' @param initials optional initials vector to prune shards
#' @param where_sql optional SQL WHERE clause to apply server-side
#' @param columns optional columns to select (default some key fields)
tnp_lake_read <- function(con, man, state, initials = NULL,
                          where_sql = NULL,
                          columns = c("npi","first_name","last_name","state")) {
  urls <- tnp_urls(man, state, initials)
  stopifnot(length(urls) > 0)

  urls_sql <- paste(sprintf("'%s'", urls), collapse = ", ")
  sel <- paste(columns, collapse = ", ")
  base <- sprintf("SELECT %s FROM parquet_scan([%s])", sel, urls_sql)
  if (!is.null(where_sql)) base <- paste(base, "WHERE", where_sql)

  DBI::dbGetQuery(con, base)
}
