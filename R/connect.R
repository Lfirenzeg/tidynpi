#' Open a DuckDB connection configured for HTTP(S) parquet
#'
#' @param dbdir Path to DuckDB database directory (default ":memory:").
#' @return A DBI connection to DuckDB with httpfs loaded.
#' @export
tnp_duckdb <- function(dbdir = ":memory:") {
  con <- DBI::dbConnect(duckdb::duckdb(dbdir))
  DBI::dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
  # httpfs uses libcurl; no S3 config needed for public HTTPS
  con
}
