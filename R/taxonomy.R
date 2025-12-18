#' Taxonomy dictionary URL (public)
#' @export
tnp_taxonomy_url <- function() {
  "https://rnppes.org/nppes/nucc_taxonomy_251.csv"
}

.tnp_tax_cache <- new.env(parent = emptyenv())

#' Download and cache the NUCC taxonomy dictionary
#' @param url Optional override URL
#' @export
tnp_taxonomy_dict <- function(url = tnp_taxonomy_url()) {
  if (exists("dict", envir = .tnp_tax_cache, inherits = FALSE)) {
    return(get("dict", envir = .tnp_tax_cache, inherits = FALSE))
  }

  tmp <- tempfile(fileext = ".csv")
  utils::download.file(url, tmp, quiet = TRUE)

  dict <- utils::read.csv(tmp, stringsAsFactors = FALSE, check.names = FALSE)

  # Expect columns like: Code, Grouping, Classification, Specialization
  # Normalize names
  names(dict) <- tolower(gsub("[^A-Za-z0-9]+", "_", names(dict)))

  # Commonize
  code_col <- if ("code" %in% names(dict)) "code" else names(dict)[1]
  grouping <- if ("grouping" %in% names(dict)) "grouping" else NA_character_
  classification <- if ("classification" %in% names(dict)) "classification" else NA_character_
  specialization <- if ("specialization" %in% names(dict)) "specialization" else NA_character_

  out <- data.frame(
    code = dict[[code_col]],
    grouping = if (!is.na(grouping)) dict[[grouping]] else NA_character_,
    classification = if (!is.na(classification)) dict[[classification]] else NA_character_,
    specialization = if (!is.na(specialization)) dict[[specialization]] else NA_character_,
    stringsAsFactors = FALSE
  )

  # A helpful display label
  out$display_name <- ifelse(
    is.na(out$specialization) | out$specialization == "",
    paste0(out$classification, " ", out$grouping),
    paste0(out$specialization, " (", out$classification, ")")
  )

  assign("dict", out, envir = .tnp_tax_cache)
  out
}

#' Translate taxonomy code columns into readable labels
#' @param x A data.frame/tibble with columns like tax_code_1, tax_code_2, ...
#' @param cols Which columns to translate (default: all columns starting with "tax_code_")
#' @export
tnp_taxonomy_translate <- function(x, cols = NULL) {
  dict <- tnp_taxonomy_dict()

  if (is.null(cols)) {
    cols <- names(x)[grepl("^tax_code_\\d+$", names(x))]
  }
  if (length(cols) == 0) return(x)

  for (cc in cols) {
    m <- match(x[[cc]], dict$code)
    x[[paste0("tax_", cc, "_classification")]] <- dict$classification[m]
    x[[paste0("tax_", cc, "_specialization")]] <- dict$specialization[m]
    x[[paste0("tax_", cc, "_display_name")]]   <- dict$display_name[m]
  }
  x
}
