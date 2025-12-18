# internal: read a (state, initial) partition once, and gracefully drop missing columns
npi_read_block <- function(con, man, state_part, lname_initial, columns, ...) {
  stopifnot(!is.null(con), !is.null(man))

  reader <- NULL
  if (exists("tnp_lake_read_https", mode = "function")) {
    reader <- get("tnp_lake_read_https", mode = "function")
  } else if (exists("tnp_lake_read", mode = "function")) {
    reader <- get("tnp_lake_read", mode = "function")
  } else {
    stop("Need tnp_lake_read_https() or tnp_lake_read() in the package.")
  }

  cols <- unique(columns)

  # iterative retry for missing columns (Binder Error: Referenced column ...)
  for (attempt in 1:10) {
    out <- try(do.call(reader, c(
      list(con = con, man = man, state = state_part, initials = lname_initial, columns = cols),
      list(...)
    )), silent = TRUE)

    if (!inherits(out, "try-error")) return(out)

    msg <- conditionMessage(attr(out, "condition"))
    m <- regexec('Referenced column "([^"]+)" not found', msg)
    hit <- regmatches(msg, m)[[1]]
    if (length(hit) == 2) {
      bad <- hit[2]
      cols <- setdiff(cols, bad)
      if (length(cols) == 0) return(data.frame())
      next
    }
    # not a missing-column error → rethrow
    stop(msg, call. = FALSE)
  }

  data.frame()
}

# internal: assign rank/label per your rubric
npi_rank <- function(exact_name, jw, city_match) {
  # city_match: TRUE / FALSE / NA (unknown)
  if (isTRUE(exact_name) && isTRUE(city_match)) return(c(1, "Extremely Likely Match"))
  if (isTRUE(exact_name) && !isTRUE(city_match)) return(c(2, "Very Likely Match"))
  if (!isTRUE(exact_name) && jw >= 0.95 && isTRUE(city_match)) return(c(3, "Likely Match"))
  if (!isTRUE(exact_name) && jw >= 0.95 && !isTRUE(city_match)) return(c(4, "Possible Match"))
  if (jw >= 0.90) return(c(5, "Possible Match"))
  c(NA, NA)
}

#' Enrich/match input names against the public lake snapshot
#' @param inputs data.frame/tibble with full_name + state (and optional city)
#' @param con DuckDB connection (tnp_duckdb())
#' @param man Manifest object (tnp_manifest())
#' @param strategy "strict" uses jw>=0.95; "permissive" uses jw>=0.90
#' @param city_mode "prefer" ranks with city when available; "ignore" ignores city; "require" filters to city matches when both sides have city
#' @param max_candidates cap candidates per input_id
#' @param ... passed through to tnp_lake_read_https (e.g., max_urls_per_query, tries, backoff params)
#' @param verbose Logical. If TRUE, prints progress messages while processing each (state, initial) block.
#' @export
npi_enrich <- function(inputs,
                       con,
                       man,
                       strategy = c("strict","permissive"),
                       city_mode = c("prefer","ignore","require"),
                       max_candidates = 20,
                       verbose = FALSE,
                       ...) {

  strategy  <- match.arg(strategy)
  city_mode <- match.arg(city_mode)

  norm <- npi_normalize(inputs, full_name = "full_name", state = "state", city = "city")

  # group keys
  keys <- unique(norm[, c("state_part","lname_initial")])
  keys <- keys[order(keys$state_part, keys$lname_initial), , drop = FALSE]

  t0 <- Sys.time()

  res_all <- list()
  idx <- 0L

  # columns we *wish* to read (v2 won’t have city/tax; v3 will)
  want_cols <- c(
    "npi","entity_type","first_name","last_name_leg","last_name","org_name","state","city",
    "tax_code_1","tax_code_2","tax_code_3","tax_code_4","tax_code_5",
    "state_part","lname_initial","name_for_initial","lname"
  )

  for (k in seq_len(nrow(keys))) {
    st  <- keys$state_part[k]
    ini <- keys$lname_initial[k]
    block_inputs <- norm[norm$state_part == st & norm$lname_initial == ini, , drop = FALSE]
    if (nrow(block_inputs) == 0) next

    if (isTRUE(verbose)) {
      elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
      message(sprintf(
        "[npi_enrich] %d/%d  state=%s initial=%s  inputs=%d  elapsed=%.1fs",
        k, nrow(keys), st, ini, nrow(block_inputs), elapsed
      ))
    }

    cand <- npi_read_block(con, man, st, ini, columns = want_cols, ...)


    if (is.null(cand) || nrow(cand) == 0) next

    # prefer individuals
    if ("entity_type" %in% names(cand)) {
      cand <- cand[is.na(cand$entity_type) | cand$entity_type == 1, , drop = FALSE]
    }
    if (nrow(cand) == 0) next

    # candidate name fields
    cand_first <- if ("first_name" %in% names(cand)) cand$first_name else ""
    cand_last  <- if ("last_name_leg" %in% names(cand)) cand$last_name_leg else if ("last_name" %in% names(cand)) cand$last_name else ""

    cand$cand_first_norm <- tnp_norm_upper(cand_first)
    cand$cand_last_norm  <- tnp_norm_upper(cand_last)
    cand$cand_full_norm  <- paste(cand$cand_first_norm, cand$cand_last_norm)
    cand$cand_first_initial <- substr(cand$cand_first_norm, 1, 1)

    if ("city" %in% names(cand)) {
      cand$cand_city_norm <- tnp_norm_upper(cand$city)
    } else {
      cand$cand_city_norm <- NA_character_
    }

    # match each input row in this block
    for (i in seq_len(nrow(block_inputs))) {
      inr <- block_inputs[i, , drop = FALSE]
      in_full <- paste(inr$first_norm, inr$last_norm)
      in_fi   <- substr(inr$first_norm, 1, 1)

      # light blocking by first initial when available
      cand_sub <- cand
      if (!is.na(in_fi) && in_fi != "") {
        keep_fi <- is.na(cand_sub$cand_first_initial) | cand_sub$cand_first_initial == "" | cand_sub$cand_first_initial == in_fi
        cand_sub <- cand_sub[keep_fi, , drop = FALSE]
      }
      if (nrow(cand_sub) == 0) next

      # JW similarity (0..1). Use stringdist (exported, stable) rather than
      # stringi internals (which are not guaranteed to be exported).
      jw <- stringdist::stringsim(
        rep(in_full, nrow(cand_sub)),
        cand_sub$cand_full_norm,
        method = "jw",
        p = 0.1
      )
      jw[is.na(jw)] <- 0

      exact_name <- (inr$first_norm == cand_sub$cand_first_norm) & (inr$last_norm == cand_sub$cand_last_norm)
      exact_name[is.na(exact_name)] <- FALSE

      # city match (3-state: TRUE/FALSE/NA)
      city_match <- NA
      if (city_mode != "ignore") {
        if (!is.na(inr$city_norm) && inr$city_norm != "" &&
            !all(is.na(cand_sub$cand_city_norm))) {

          # if cand city known, compute match; otherwise stay NA
          city_match <- ifelse(!is.na(cand_sub$cand_city_norm) & cand_sub$cand_city_norm != "",
                               cand_sub$cand_city_norm == inr$city_norm,
                               NA)
        }
      }

      # thresholds
      jw_min <- if (strategy == "strict") 0.95 else 0.90
      keep <- exact_name | (jw >= jw_min)

      # if require city, only filter when BOTH sides have city
      if (city_mode == "require") {
        have_both <- !is.na(city_match)
        keep <- keep & (!have_both | (city_match == TRUE))
      }

      if (!any(keep)) next

      cand_hit <- cand_sub[keep, , drop = FALSE]
      jw_hit   <- jw[keep]
      ex_hit   <- exact_name[keep]
      cm_hit   <- if (length(city_match) == 1 && is.na(city_match)) rep(NA, nrow(cand_hit)) else city_match[keep]

      ranks <- mapply(npi_rank, ex_hit, jw_hit, cm_hit, SIMPLIFY = FALSE)
      rank_num   <- as.integer(vapply(ranks, `[`, character(1), 1))
      rank_label <- vapply(ranks, `[`, character(1), 2)

      out <- cand_hit
      out$input_id    <- inr$input_id
      out$first_norm  <- inr$first_norm
      out$last_norm   <- inr$last_norm
      out$city_norm   <- inr$city_norm

      out$jaro_winkler <- jw_hit
      out$exact_name   <- ex_hit
      out$city_match   <- cm_hit
      out$rank_num     <- rank_num
      out$rank_label   <- rank_label

      # sort + cap
      ord <- order(out$rank_num, -out$jaro_winkler, na.last = TRUE)
      out <- out[ord, , drop = FALSE]
      if (nrow(out) > max_candidates) out <- out[seq_len(max_candidates), , drop = FALSE]

      idx <- idx + 1L
      res_all[[idx]] <- out
    }
  }

  if (length(res_all) == 0) {
    # return an empty data.frame with expected columns if possible
    return(data.frame())
  }

  dplyr::bind_rows(res_all)
}
