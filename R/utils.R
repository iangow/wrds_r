#' Check connection validity
#'
#' Internal function to validate a database connection object.
#'
#' @param wrds Object to check.
#'
#' @return Invisibly returns `TRUE` if valid; aborts otherwise.
#'
#' @keywords internal
check_connection <- function(wrds) {
  if (!inherits(wrds, "DBIConnection")) {
    cli::cli_abort(c(
      "{.arg wrds} must be a database connection.",
      "i" = "Use {.fn wrds_connect} to create a connection."
    ))
  }

  if (!DBI::dbIsValid(wrds)) {
    cli::cli_abort(c(
      "Database connection is no longer valid.",
      "i" = "Use {.fn wrds_connect} to create a new connection."
    ))
  }

  invisible(TRUE)
}

#' Smart collect with size awareness
#'
#' Internal function that collects a lazy table with warnings for large queries.
#'
#' @param tbl A `tbl_lazy` object from dbplyr.
#' @param wrds Database connection for row counting.
#' @param lazy If `TRUE`, return the lazy table without collecting.
#'
#' @return A tibble if collecting, or the lazy table if `lazy = TRUE`.
#'
#' @keywords internal
smart_collect <- function(tbl, wrds, lazy = FALSE) {
  if (lazy) {
    return(tbl)
  }

  threshold <- getOption("wrds.collect_threshold", 200000L)
  abort_threshold <- getOption("wrds.abort_threshold", 2000000L)

  nrow <- tryCatch(
    tbl |>
      dplyr::ungroup() |>
      dplyr::tally() |>
      dplyr::pull(n),
    error = \(e) NA_integer_
  )

  if (!is.na(nrow) && nrow > abort_threshold) {
    cli::cli_abort(c(
      "Query would return {.strong {format(nrow, big.mark = ',')}} rows.",
      "i" = "Use {.code lazy = TRUE} to work with the data remotely.",
      "i" = "Or filter to reduce the result size."
    ))
  }

  if (!is.na(nrow) && nrow > threshold) {
    cli::cli_alert_warning(
      "Query returns {.strong {format(nrow, big.mark = ',')}} rows. \
      Consider using {.code lazy = TRUE} for large queries."
    )
  }

  dplyr::collect(tbl)
}
