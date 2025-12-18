#' Normalize simple person names
#'
#' @param x Character vector of names to normalize.
#' @return Uppercased, ASCII-like, squished name strings.
#' @export
tnp_norm_name <- function(x) {
  x <- stringi::stri_trans_general(x, "Any-Upper")
  x <- gsub("[^A-Z\\s'-]", " ", x)
  x <- gsub("\\s+", " ", trimws(x))
  x
}
