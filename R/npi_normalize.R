# internal: normalize strings consistently
tnp_norm_upper <- function(x) {
  x <- ifelse(is.na(x), "", x)
  # normalize apostrophe variants to straight apostrophe
  x <- gsub("[\u2019\u2018\u02BC\u02BB\u0060\u00B4]", "'", x, perl = TRUE)
  # remove apostrophes BETWEEN letters: D'Angelo -> DAngelo, O'Connor -> OConnor
  x <- gsub("(?<=[[:alpha:]])'(?=[[:alpha:]])", "", x, perl = TRUE)
  x <- stringi::stri_trans_general(x, "Latin-ASCII")
  x <- toupper(x)
  x <- gsub("[^A-Z0-9 ]+", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

# internal: split full name into first/last (simple, robust for demos)
tnp_split_full_name <- function(full_name) {
  raw0 <- ifelse(is.na(full_name), "", full_name)

  # Join apostrophes *between letters* so D'Angela -> DAngela, O'Connor -> OConnor
  raw <- gsub("(?<=[[:alpha:]])[\u2019\u2018\u02BC\u02BB'](?=[[:alpha:]])", "", raw0, perl = TRUE)

  if (grepl(",", raw, fixed = TRUE)) {
    parts <- stringi::stri_split_fixed(raw, ",", n = 2, simplify = TRUE)
    last  <- tnp_norm_upper(parts[,1])
    rest  <- tnp_norm_upper(parts[,2])
    toks  <- strsplit(rest, " ", fixed = TRUE)[[1]]
    first <- if (length(toks) >= 1) toks[1] else ""
    return(list(first = first, last = last))
  }

  nm <- tnp_norm_upper(raw)
  toks <- strsplit(nm, " ", fixed = TRUE)[[1]]
  toks <- toks[toks != ""]

  # drop common suffixes/titles at end
  suffixes <- c("JR","SR","II","III","IV","MD","DO","PHD","DDS","DMD","RN","NP","PA","MPH","MBA")
  while (length(toks) > 1 && utils::tail(toks, 1) %in% suffixes) {
    toks <- utils::head(toks, -1)
  }

  first <- if (length(toks) >= 1) toks[1] else ""
  last  <- if (length(toks) >= 2) toks[length(toks)] else ""
  list(first = first, last = last)
}

#' Normalize user input rows for lake search & matching
#' @param inputs data.frame/tibble with at least full_name + state (and optional city)
#' @param full_name Column name for full name
#' @param state Column name for state (2-letter preferred)
#' @param city Column name for city (optional)
#' @export
npi_normalize <- function(inputs, full_name = "full_name", state = "state", city = "city") {
  x <- inputs

  if (!("input_id" %in% names(x))) x$input_id <- seq_len(nrow(x))

  spl <- lapply(x[[full_name]], tnp_split_full_name)
  x$first_norm <- vapply(spl, `[[`, character(1), "first")
  x$last_norm  <- vapply(spl, `[[`, character(1), "last")

  x$state <- tnp_norm_upper(x[[state]])
  x$city_norm <- if (city %in% names(x)) tnp_norm_upper(x[[city]]) else ""

  valid_codes <- c(
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC","PR","GU","VI","AS","MP","AE","AP","AA"
  )

  x$state_part <- ifelse(nchar(x$state) == 2 & x$state %in% valid_codes, x$state, "INTL")

  # initial encoding compatible with your snapshot convention
  first_char <- substr(x$last_norm, 1, 1)
  x$lname_initial <- ifelse(
    first_char == "", "_",
    ifelse(first_char == "#", "%23",
           ifelse(first_char == "/", "%2F",
                  ifelse(grepl("^[A-Z0-9]$", first_char), first_char, "_")))
  )

  x
}
