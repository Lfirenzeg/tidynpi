#' @importFrom stats setNames
NULL

# Helpers

tnp_valid_state_codes <- function() {
  c(
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC","PR","GU","VI","AS","MP","AE","AP","AA","INTL"
  )
}

# Map common full state names -> abb (50 states), plus DC; anything else -> INTL
tnp_state_to_part <- function(state) {
  if (is.null(state)) return(NA_character_)
  s <- toupper(trimws(as.character(state)))
  s[s == ""] <- NA_character_

  valid <- tnp_valid_state_codes()

  # Already 2-letter
  out <- ifelse(!is.na(s) & nchar(s) == 2 & s %in% valid, s, NA_character_)

  # Full state names
  need <- is.na(out) & !is.na(s)
  if (any(need)) {
    # 50-state mapping via built-ins
    m <- setNames(datasets::state.abb, toupper(datasets::state.name))
    out[need] <- m[s[need]]
    # DC common strings
    out[need & is.na(out)] <- ifelse(
      grepl("DISTRICT OF COLUMBIA|WASHINGTON,?\\s*DC|\\bDC\\b", s[need]),
      "DC",
      NA_character_
    )
  }

  out[is.na(out)] <- "INTL"
  out
}

tnp_norm_token <- function(x) {
  x <- toupper(trimws(as.character(x)))
  x[x == ""] <- NA_character_
  # Keep A–Z, 0–9 and spaces only
  x <- gsub("[^A-Z0-9 ]+", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

tnp_parse_full_name <- function(x) {
  # Supports "Last, First Middle" or "First Middle Last"
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_

  first <- last <- middle <- rep(NA_character_, length(x))

  has_comma <- !is.na(x) & grepl(",", x)
  if (any(has_comma)) {
    parts <- strsplit(x[has_comma], ",", fixed = TRUE)
    last[has_comma]  <- trimws(vapply(parts, `[[`, "", 1))
    rest <- trimws(vapply(parts, function(p) if (length(p) >= 2) p[2] else "", ""))
    rest_parts <- strsplit(rest, "\\s+")
    first[has_comma] <- trimws(vapply(rest_parts, function(p) if (length(p) >= 1) p[1] else "", ""))
    middle[has_comma] <- trimws(vapply(rest_parts, function(p) if (length(p) >= 3) paste(p[2:(length(p)-1)], collapse=" ") else "", ""))
    middle[has_comma][middle[has_comma] == ""] <- NA_character_
  }

  no_comma <- !is.na(x) & !has_comma
  if (any(no_comma)) {
    parts <- strsplit(x[no_comma], "\\s+")
    first[no_comma] <- trimws(vapply(parts, function(p) if (length(p) >= 1) p[1] else "", ""))
    last[no_comma]  <- trimws(vapply(parts, function(p) if (length(p) >= 2) p[length(p)] else "", ""))
    middle[no_comma] <- trimws(vapply(parts, function(p) if (length(p) >= 3) paste(p[2:(length(p)-1)], collapse=" ") else "", ""))
    middle[no_comma][middle[no_comma] == ""] <- NA_character_
  }

  data.frame(first = first, middle = middle, last = last, stringsAsFactors = FALSE)
}

tnp_encode_initial <- function(x) {
  x <- toupper(trimws(as.character(x)))
  x[x == "" | is.na(x)] <- "_"
  ch <- substr(x, 1, 1)
  out <- ifelse(ch == "#", "%23",
                ifelse(ch == "/", "%2F",
                       ifelse(grepl("^[A-Z0-9]$", ch), ch, "_")))
  out
}

tnp_lake_columns <- local({
  cache <- new.env(parent = emptyenv())
  function(con, one_url) {
    key <- one_url
    if (exists(key, envir = cache, inherits = FALSE)) return(get(key, envir = cache, inherits = FALSE))
    # Minimal query: schema only
    sql <- sprintf("SELECT * FROM parquet_scan(['%s']) LIMIT 0", one_url)
    cols <- names(DBI::dbGetQuery(con, sql))
    assign(key, cols, envir = cache)
    cols
  }
})

#  Public API (internal helper version with expanded parameters)

# Internal normalization function with separate first/last name support
npi_normalize <- function(
    x,
    full_name = "full_name",
    first_name = "first_name",
    last_name = "last_name",
    state = "state",
    city = "city"
) {
  if (!is.data.frame(x)) stop("x must be a data.frame")
  if (!state %in% names(x)) stop("Missing state column: ", state)

  out <- x
  out$input_id <- seq_len(nrow(out))

  # Name source: full_name OR first/last
  have_full <- full_name %in% names(out) && any(!is.na(out[[full_name]]) & trimws(out[[full_name]]) != "")
  if (have_full) {
    parsed <- tnp_parse_full_name(out[[full_name]])
    out$first_name <- parsed$first
    out$last_name  <- parsed$last
    out$middle_name <- parsed$middle
  } else {
    if (!first_name %in% names(out) || !last_name %in% names(out)) {
      stop("Provide either a full_name column or both first_name and last_name columns.")
    }
    out$first_name <- out[[first_name]]
    out$last_name  <- out[[last_name]]
    out$middle_name <- NA_character_
  }

  out$state_part <- tnp_state_to_part(out[[state]])

  out$first_norm <- tnp_norm_token(out$first_name)
  out$last_norm  <- tnp_norm_token(out$last_name)

  if (city %in% names(out)) {
    out$city_norm <- tnp_norm_token(out[[city]])
  } else {
    out$city_norm <- NA_character_
  }

  out$lname_initial <- tnp_encode_initial(out$last_norm)

  out
}

#' Search the public Parquet lake for candidate providers
#'
#' For each input row, reads only the relevant (state_part + lname_initial) shards from the manifest,
#' and applies pushdown filters (exact last name; optional city) before returning candidates.
#'
#' @param con A DuckDB connection (from tnp_duckdb()).
#' @param man Manifest list (from tnp_manifest()).
#' @param x Normalized inputs (from npi_normalize()).
#' @param max_urls_per_query Batch size for URL reads (helps with rate limits).
#' @param tries Retry attempts (HTTP 429 mitigation).
#' @param initial_wait Initial backoff wait seconds.
#' @param max_wait Max backoff wait seconds.
#' @param sleep_between_batches Sleep between URL batches.
#' @param verbose Print progress.
#' @return A data.frame of candidates with input_id attached.
#' @export
npi_search_lake <- function(
    con, man, x,
    max_urls_per_query = 3,
    tries = 6,
    initial_wait = 0.8,
    max_wait = 8,
    sleep_between_batches = 0.2,
    verbose = FALSE
) {
  if (!is.data.frame(x) || !"input_id" %in% names(x)) {
    stop("x must be the output of npi_normalize() (must include input_id).")
  }

  out_list <- vector("list", nrow(x))

  for (i in seq_len(nrow(x))) {
    st <- x$state_part[i]
    ini <- x$lname_initial[i]

    urls <- tnp_urls(man, st, initials = ini)
    if (length(urls) == 0) {
      out_list[[i]] <- NULL
      next
    }

    # Detect available columns from the first shard (v2 vs v3 compatibility)
    cols_avail <- tnp_lake_columns(con, urls[1])

    want <- c("npi", "first_name", "last_name_leg", "state", "state_part", "lname_initial",
              "city", "tax_code_1","tax_code_2","tax_code_3","tax_code_4","tax_code_5",
              "entity_type", "name_for_initial", "lname")

    cols <- intersect(want, cols_avail)

    # Pushdown filters: exact last name; entity_type=1 if present; optional city if present
    where <- character(0)

    if ("entity_type" %in% cols_avail) {
      where <- c(where, "entity_type = 1")
    }

    # last name exact filter (use lname if available, else upper(last_name_leg))
    ln <- x$last_norm[i]
    if (!is.na(ln) && nzchar(ln)) {
      if ("lname" %in% cols_avail) {
        where <- c(where, sprintf("lname = '%s'", gsub("'", "''", ln)))
      } else if ("last_name_leg" %in% cols_avail) {
        where <- c(where, sprintf("upper(last_name_leg) = '%s'", gsub("'", "''", ln)))
      }
    }

    # city filter (only if user provided AND column exists)
    ct <- x$city_norm[i]
    if (!is.na(ct) && nzchar(ct) && "city" %in% cols_avail) {
      where <- c(where, sprintf("upper(city) = '%s'", gsub("'", "''", ct)))
    }

    where_sql <- if (length(where)) paste(where, collapse = " AND ") else NULL

    res <- tnp_lake_read_https(
      con, man,
      state = st,
      initials = ini,
      columns = cols,
      where_sql = where_sql,
      max_urls_per_query = max_urls_per_query,
      tries = tries,
      initial_wait = initial_wait,
      max_wait = max_wait,
      sleep_between_batches = sleep_between_batches,
      verbose = verbose
    )

    if (nrow(res)) {
      res$input_id <- x$input_id[i]
    }
    out_list[[i]] <- res
  }

  dplyr::bind_rows(out_list)
}
